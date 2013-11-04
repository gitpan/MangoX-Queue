package MangoX::Queue;

use Mojo::Base 'Mojo::EventEmitter';

use Carp 'croak';
use Mojo::Log;
use Mango::BSON ':bson';
use MangoX::Queue::Delay;
use DateTime::Tiny;

our $VERSION = '0.06';

# A logger
has 'log' => sub { Mojo::Log->new->level('error') };

# The Mango::Collection representing the queue
has 'collection';
has 'capped';
has 'stats';

# A MangoX::Queue::Delay
has 'delay' => sub { MangoX::Queue::Delay->new };

# How long to wait before assuming a job has failed
has 'timeout' => sub { $ENV{MANGOX_QUEUE_JOB_TIMEOUT} // 60 };

# How many times to retry a job before giving up
has 'retries' => sub { $ENV{MANGOX_QUEUE_JOB_RETRIES} // 5 };

# Store Mojo::IOLoop->timer IDs
has 'consumers' => sub { {} };

# Store plugins
has 'plugins' => sub { {} };

sub new {
    my $self = shift->SUPER::new(@_);

    croak qq{No Mango::Collection provided to constructor} unless ref($self->collection) eq 'Mango::Collection';

    $self->stats($self->collection->stats);
    $self->capped($self->stats->{capped});

    $self->{pending_status} = $self->capped ? 1 : 'Pending';
    $self->{processing_status} = $self->capped ? 2 : 'Processing';
    $self->{failed_status} = $self->capped ? 3 : 'Failed';

    return $self;
}

sub plugin {
    my ($self, $name, $options) = @_;

    croak qq{Plugin $name already loaded} if exists $self->plugins->{$name};

    {
        no strict 'refs';
        unless($name->can('new')) {
            eval "require $name" or croak qq{Failed to load plugin $name: $@};
        }
    }

    eval {
        $self->plugins->{$name} = $name->new(%$options);  
        return 1;          
    } or croak qq{Error calling constructor for plugin $name: $@};

    eval {
        $self->plugins->{$name}->register($self);
        return 1;
    } or croak qq{Error calling register for plugin $name: $@};

    return $self->plugins->{$name};
}

sub get_options {
    my ($self) = @_;

    return {
        query => {
            '$or' => [{
                status => {
                    '$in' => ref($self->{pending_status}) eq 'ARRAY' ? $self->{pending_status} : [ $self->{pending_status} ],
                },
                '$or' => [ { processing => 0 }, { processing => undef } ],
            },{
                status => $self->{processing_status},
                processing => {
                    '$lt' => time - $self->timeout,
                }
            }],
            attempt => {
                '$lte' => $self->retries + 1,
            },
        },
        sort => bson_doc( # Sort by priority, then in order of creation
            'priority' => 1,
            'created' => -1,
        ),
        update => {
            '$set' => {
                processing => time,
                status => $self->{processing_status},
            },
            '$inc' => {
                attempt => 1,
            }
        }
    };
}

sub enqueue {
    my ($self, @args) = @_;

    # args maybe
    # - 'job_name'
    # - foo => bar, 'job_name'
    # - 'job_name', $callback
    # - foo => bar, 'job_name', $callback

    my $callback = ref($args[-1]) eq 'CODE' ? pop @args : undef;
    my $job = pop @args;
    my %args;
    %args = (@args) if scalar @args;

    my $db_job = {
        priority => $args{priority} // 1,
        created => $args{created} // DateTime::Tiny->now,
        data => $job,
        status => $args{status} // $self->{pending_status},
        attempt => 1,
        processing => 0,
    };

    if($callback) {
        return $self->collection->insert($db_job => sub {
            my ($collection, $error, $oid) = @_;
            if($error) {
                $self->emit_safe(error => qq{Error inserting job into collection: $error});
                return;
            }
            $db_job->{_id} = $oid;
            $self->emit_safe(enqueued => $db_job) if $self->has_subscribers('enqueued');
            eval {
                $callback->($db_job);
                return 1;
            } or $self->emit_safe(error => qq{Error in callback: $@});
        });
    } else {
        eval {
            $db_job->{_id} = $self->collection->insert($db_job);
            return 1;
        } or croak qq{Error inserting job into collection: $@};
        $self->emit_safe(enqueued => $db_job) if $self->has_subscribers('enqueued');
        return $db_job;
    }
}

sub watch {
    my ($self, $id_or_job, $status, $callback) = @_;

    my $id = ref($id_or_job) ? $id_or_job->{_id} : $id_or_job;

    $status //= 'Complete';

    # args
    # - watch $queue $id, 'Status' => $callback

    if($callback) {
        # Non-blocking
        $self->log->debug("Waiting for $id on status $status in non-blocking mode");
        return Mojo::IOLoop->timer(0 => sub { $self->_watch_nonblocking($id, $status, $callback) });
    } else {
        # Blocking
        $self->log->debug("Waiting for $id on status $status in blocking mode");
        return $self->_watch_blocking($id, $status);
    }
}

sub _watch_blocking {
    my ($self, $id, $status) = @_;

    while(1) {
        my $doc = $self->collection->find_one({'_id' => $id});
        $self->log->debug("Job found by Mango: " . ($doc ? 'Yes' : 'No'));

        if($doc && ((!ref($status) && $doc->{status} eq $status) || (ref($status) eq 'ARRAY' && grep { $_ =~ $doc->{status} } @$status))) {
            return 1;
        } else {
            $self->delay->wait;
        }
    }
}

sub _watch_nonblocking {
    my ($self, $id, $status, $callback) = @_;

    $self->collection->find_one({'_id' => $id} => sub {
        my ($cursor, $err, $doc) = @_;
        $self->log->debug("Job found by Mango: " . ($doc ? 'Yes' : 'No'));
        
        if($doc && ((!ref($status) && $doc->{status} eq $status) || (ref($status) eq 'ARRAY' && grep { $_ =~ $doc->{status} } @$status))) {
            $self->log->debug("Status is $status");
            $self->delay->reset;
            $callback->($doc);
        } else {
            $self->log->debug("Job not found or status doesn't match");
            $self->delay->wait(sub {
                return unless Mojo::IOLoop->is_running;
                Mojo::IOLoop->timer(0 => sub { $self->_watch_nonblocking($id, $status, $callback) });
            });
            return undef;
        }
    });
}

sub requeue {
    my ($self, $job, $callback) = @_;

    $job->{status} = ref($self->{pending_status}) eq 'ARRAY' ? $self->{pending_status}->[0] : $self->{pending_status};
    return $self->update($job, $callback);
}

sub dequeue {
    my ($self, $id_or_job, $callback) = @_;

    # TODO option to not remove on dequeue?

    my $id = ref($id_or_job) ? $id_or_job->{_id} : $id_or_job;

    if($callback) {
        $self->collection->remove({'_id' => $id} => sub {
            $callback->();
            $self->emit_safe(dequeued => $id_or_job) if $self->has_subscribers('dequeued');
        });
    } else {
        $self->collection->remove({'_id' => $id});
        $self->emit_safe(dequeued => $id_or_job) if $self->has_subscribers('dequeued');
    }
}

sub get {
    my ($self, $id_or_job, $callback) = @_;

    my $id = ref($id_or_job) ? $id_or_job->{_id} : $id_or_job;

    if($callback) {
        return $self->collection->find_one({'_id' => $id} => sub {
            my ($collection, $error, $doc) = @_;
            $callback->($doc);
        });
    } else {
        return $self->collection->find_one({'_id' => $id});
    }
}

sub update {
    my ($self, $job, $callback) = @_;

    if($callback) {
        return $self->collection->find_one({'_id' => $job->{_id}} => sub {
            my ($collection, $error, $doc) = @_;
            if($error) {
                $self->emit_safe(error => $error);
                return;
            }
            $callback->($doc);
        });
    } else {
        return $self->collection->update({'_id' => $job->{_id}}, $job, {upsert => 1}) or croak qq{Error updating collection: $@};
    }
}

sub fetch {
    my ($self, @args) = @_;

    # fetch $queue status => 'Complete', sub { my $job = shift; }

    my $callback = ref($args[-1]) eq 'CODE' ? pop @args : undef;
    my %args;
    %args = (@args) if scalar @args;

    $self->log->debug("In fetch");

    if($callback) {
        $self->log->debug("Fetching in non-blocking mode");
        my $consumer_id = (scalar keys %{$self->consumers}) + 1;
        $self->consumers->{$consumer_id} = Mojo::IOLoop->timer(0 => sub { $self->_consume_nonblocking(\%args, $consumer_id, $callback, 1) });
        return $consumer_id;
    } else {
        $self->log->debug("Fetching in blocking mode");
        return $self->_consume_blocking(\%args, 1);
    }
}

sub consume {
    my ($self, @args) = @_;

    # consume $queue status => 'Failed', sub { my $job = shift; }

    my $callback = ref($args[-1]) eq 'CODE' ? pop @args : undef;
    my %args;
    %args = (@args) if scalar @args;

    $self->log->debug("In consume");

    if($callback) {
        $self->log->debug("consuming in non-blocking mode");
        my $consumer_id = (scalar keys %{$self->consumers}) + 1;
        $self->consumers->{$consumer_id} = Mojo::IOLoop->timer(0 => sub { $self->_consume_nonblocking(\%args, $consumer_id, $callback, 0) });
        $self->log->debug("Timer scheduled, consumer_id $consumer_id has timer ID: " . $self->consumers->{$consumer_id});
        return $consumer_id;
    } else {
        $self->log->debug("consuming in blocking mode");
        return $self->_consume_blocking(\%args, 0);
    }
}

sub release {
    my ($self, $consumer_id) = @_;

    $self->log->debug("Releasing consumer $consumer_id with timer ID: " . $self->consumers->{$consumer_id});

    Mojo::IOLoop->remove($self->consumers->{$consumer_id});
    delete $self->consumers->{$consumer_id};

    return 1;
}

sub _consume_blocking {
    my ($self, $args, $fetch) = @_;

    while(1) {
        my $opts = $self->get_options;
        $opts->{query} = $args if scalar keys %$args;

        my $doc = $self->collection->find_and_modify($opts);
        $self->log->debug("Job found by Mango: " . ($doc ? 'Yes' : 'No'));

        if($doc && $doc->{attempt} > $self->retries) {
            $doc->{status} = $self->{failed_status};
            $self->update($doc);
            $doc = undef;
            $self->log->debug("Job exceeded retries, status set to failed and job abandoned");
        }

        if($doc) {
            $self->emit_safe(consumed => $doc) if $self->has_subscribers('consumed');
            return $doc;
        } else {
            last if $fetch;
            $self->delay->wait;
        }
    }
}

sub _consume_nonblocking {
    my ($self, $args, $consumer_id, $callback, $fetch) = @_;

    my $opts = $self->get_options;
    $opts->{query} = $args if scalar keys %$args;

    $self->collection->find_and_modify($opts => sub {
        my ($cursor, $err, $doc) = @_;
        $self->log->debug("Job found by Mango: " . ($doc ? 'Yes' : 'No'));

        if($err) {
            $self->log->error($err);
            $self->emit_safe(error => $err);
        }
        
        if($doc && $doc->{attempt} > $self->retries) {
            $doc->{status} = $self->{failed_status};
            $self->update($doc);
            $doc = undef;
            $self->log->debug("Job exceeded retries, status set to failed and job abandoned");
        }

        if($doc) {
            $self->delay->reset;
            $self->emit_safe(consumed => $doc) if $self->has_subscribers('consumed');
            eval {
                $callback->($doc);
                return 1;
            } or $self->emit_safe(error => "Error in callback: $@");
            return unless Mojo::IOLoop->is_running;
            return if $fetch;
            return unless exists $self->consumers->{$consumer_id};
            #$self->consumers->{$consumer_id} = Mojo::IOLoop->timer(0 => sub { $self->_consume_nonblocking($args, $consumer_id, $callback, 0) });
            $self->_consume_nonblocking($args, $consumer_id, $callback, 0);
            $self->log->debug("Timer rescheduled (recursive immediate), consumer_id $consumer_id has timer ID: " . $self->consumers->{$consumer_id});
        } else {
            return unless Mojo::IOLoop->is_running;
            return if $fetch;
            $self->delay->wait(sub {
                return unless exists $self->consumers->{$consumer_id};
                #$self->consumers->{$consumer_id} = Mojo::IOLoop->timer(0 => sub { $self->_consume_nonblocking($args, $consumer_id, $callback, 0) });
                $self->_consume_nonblocking($args, $consumer_id, $callback, 0);
                $self->log->debug("Timer rescheduled (recursive delayed), consumer_id $consumer_id has timer ID: " . $self->consumers->{$consumer_id});
            });
            return undef;
        }
    });
}

1;

=encoding utf8

=head1 NAME

MangoX::Queue - A MongoDB queue implementation using Mango

=head1 DESCRIPTION

L<MangoX::Queue> is a MongoDB backed queue implementation using L<Mango> to support
blocking and non-blocking queues.

L<MangoX::Queue> makes no attempt to handle the L<Mango> connection, database or
collection - pass in a collection to the constructor and L<MangoX::Queue> will
use it. The collection can be plain, capped or sharded.

For an introduction to L<MangoX::Queue>, see L<MangoX::Queue::Tutorial>.

=head1 SYNOPSIS

=head2 Non-blocking

Non-blocking mode requires a running L<Mojo::IOLoop>.

    my $queue = MangoX::Queue->new(collection => $mango_collection);

    # To add a job
    enqueue $queue 'test' => sub { my $id = shift; };

    # To set options
    enqueue $queue priority => 1, created => DateTime::Tiny->now, 'test' => sub { my $id = shift; };

    # To watch for a specific job status
    watch $queue $id, 'Complete' => sub {
        # Job status is 'Complete'
    };

    # To fetch a job
    fetch $queue sub {
        my ($job) = @_;
        # ...
    };

    # To get a job by id
    get $queue $id => sub { my $job = shift; };

    # To requeue a job
    requeue $queue $job => sub { my $id = shift; };

    # To dequeue a job
    dequeue $queue $id => sub { };

    # To consume a queue
    my $consumer = consume $queue sub {
        my ($job) = @_;
        # ...
    };

    # To stop consuming a queue
    release $queue $consumer;

    # To listen for errors
    on $queue error => sub { my ($queue, $error) = @_; };

=head2 Blocking

    my $queue = MangoX::Queue->new(collection => $mango_collection);

    # To add a job
    my $id = enqueue $queue 'test';

    # To set options
    my $id = enqueue $queue priority => 1, created => DateTime::Tiny->now, 'test';

    # To watch for a specific job status
    watch $queue $id;

    # To fetch a job
    my $job = fetch $queue;

    # To get a job by id
    my $job = get $queue $id;

    # To requeue a job
    my $id = requeue $queue $job;

    # To dequeue a job
    dequeue $queue $id;

    # To consume a queue
    while(my $job = consume $queue) {
        # ...
    }

=head2 Other

    my $queue = MangoX::Queue->new(collection => $mango_collection);

    # To listen for events
    on $queue enqueued => sub ( my ($queue, $job) = @_; };
    on $queue dequeued => sub ( my ($queue, $job) = @_; };
    on $queue consumed => sub { my ($queue, $job) = @_; };

    # To register a plugin
    plugin $queue 'MangoX::Queue::Plugin::Statsd';

=head1 ATTRIBUTES

L<MangoX::Queue> implements the following attributes.

=head2 collection

    my $collection = $queue->collection;
    $queue->collection($mango->db('foo')->collection('bar'));

    my $queue = MangoX::Queue->new(collection => $collection);

The L<Mango::Collection> representing the MongoDB queue collection.

=head2 delay

    my $delay = $queue->delay;
    $queue->delay(MangoX::Queue::Delay->new);

The L<MangoX::Queue::Delay> responsible for dynamically controlling the
delay between queue queries.

=head2 plugins

    my $plugins = $queue->plugins;

Returns a hash containing the plugins registered with this queue.

=head2 retries

    my $retries = $queue->retries;
    $queue->retries(5);

The number of times a job will be picked up from the queue before it is
marked as failed.

=head2 timeout

    my $timeout = $queue->timeout;
    $queue->timeout(10);

The time (in seconds) a job is allowed to stay in Retrieved state before
it is released back into Pending state. Defaults to 60 seconds.

=head1 EVENTS

L<MangoX::Queue> inherits from L<Mojo::EventEmitter> and emits the following events.

Events are emitted only for actions on the current queue object, not the entire queue.

=head2 consumed

    on $queue consumed => sub {
        my ($queue, $job) = @_;
        # ...
    };

Emitted when an item is consumed (either via consume or fetch)

=head2 dequeued

    on $queue dequeued => sub {
        my ($queue, $job) = @_;
        # ...
    };

Emitted when an item is dequeued

=head2 enqueued

    on $queue enqueued => sub {
        my ($queue, $job) = @_;
        # ...
    };

Emitted when an item is enqueued

=head1 METHODS

L<MangoX::Queue> implements the following methods.

=head2 consume

    # In blocking mode
    while(my $job = consume $queue) {
        # ...
    }

    # In non-blocking mode
    consume $queue sub {
        my ($job) = @_;
        # ...
    };

Waits for jobs to arrive on the queue, sleeping between queue checks using L<MangoX::Queue::Delay> or L<Mojo::IOLoop>.

Currently sets the status to 'Retrieved' before returning the job.

=head2 dequeue

    my $job = fetch $queue;
    dequeue $queue $job;

Dequeues a job. Currently removes it from the collection.

=head2 enqueue

    my $id = enqueue $queue 'job name';
    my $id = enqueue $queue [ 'some', 'data' ];
    my $id = enqueue $queue +{ foo => 'bar' };

Add an item to the queue in blocking mode. The default priority is 1 and status is 'Pending'.

You can set queue options including priority, created and status.

    my $id = enqueue $queue,  
        priority => 1,
        created => time,
        status => 'Pending',
        +{
            foo => 'bar'
        };

For non-blocking mode, pass in a coderef as the final argument.

    my $id = enqueue $queue 'job_name' => sub {
        # ...
    };

    my $id = enqueue $queue priority => 1, +{
        foo => 'bar',
    } => sub {
        # ...
    };

Sets the status to 'Pending' by default.

=head2 fetch

    # In blocking mode
    my $job = fetch $queue;

    # In non-blocking mode
    fetch $queue sub {
        my ($job) = @_;
        # ...
    };

Fetch a single job from the queue, returning undef if no jobs are available.

Currently sets job status to 'Retrieved'.

=head2 get

    # In non-blocking mode
    get $queue $id => sub {
        my ($job) = @_;
        # ...
    };

    # In blocking mode
    my $job = get $queue $id;

Gets a job from the queue by ID. Doesn't change the job status.

You can also pass in a job instead of an ID.

    $job = get $queue $job;

=head2 get_options

    my $options = $queue->get_options;

Returns the L<Mango::Collection> options hash used by find_and_modify to
identify and update available queue items.

=head2 release

    my $consumer = consume $queue sub {
        # ...
    };
    release $queue $consumer;

Releases a non-blocking consumer from watching a queue.

=head2 requeue

    my $job = fetch $queue;
    requeue $queue $job;

Requeues a job. Sets the job status to 'Pending'.

=head2 update

    my $job = fetch $queue;
    $job->{status} = 'Failed';
    update $queue $job;

Updates a job in the queue.

=head2 watch

Wait for a job to enter a certain status.

    # In blocking mode
    my $id = enqueue $queue 'test';
    watch $queue $id, 'Complete'; # blocks until job is complete

    # In non-blocking mode
    my $id = enqueue $queue 'test';
    watch $queue $id, 'Complete' => sub {
        # ...
    };

=head1 SEE ALSO

L<MangoX::Queue::Tutorial>, L<Mojolicious>, L<Mango>

=cut
