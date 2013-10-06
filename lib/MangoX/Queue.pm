package MangoX::Queue;

use Mojo::Base -base;

use Carp 'croak';
use DateTime;
use DateTime::Duration;
use Mojo::Log;
use Mango::BSON ':bson';
use MangoX::Queue::Delay;

no warnings 'experimental::smartmatch';

our $VERSION = '0.02';

# A logger
has 'log' => sub { Mojo::Log->new->level('error') };

# The Mango::Collection representing the queue
has 'collection';

# A MangoX::Queue::Delay
has 'delay' => sub { MangoX::Queue::Delay->new };

# How long to wait before assuming a job has failed
has 'timeout' => sub { $ENV{MANGOX_QUEUE_JOB_TIMEOUT} // 60 };

# How many times to retry a job before giving up
has 'retries' => sub { $ENV{MANGOX_QUEUE_JOB_RETRIES} // 5 };

sub new {
	my $self = shift->SUPER::new(@_);

	croak qq{No Mango::Collection provided to constructor} unless ref($self->collection) eq 'Mango::Collection';

	return $self;
}

sub get_options {
	my ($self) = @_;

	return {
		query => {
			'$or' => [{
				status => {
					'$in' => [ 'Pending' ]
				}
			},{
				status => {
					'$in' => [ 'Retrieved' ]
				},
				retrieved => {
					'$lt' => DateTime->now->subtract_duration(DateTime::Duration->new(seconds => $self->timeout))
				}
			}]
		},
		update => {
			'$set' => {
				status => 'Retrieved',
				retrieved => DateTime->now,
			},
			'$inc' => {
				attempt => 1,
			}
		},
		sort => bson_doc( # Sort by priority, then in order of creation
			'priority' => 1,
			'created' => -1,
		),
		new => 0, # Get the original object (so we can see status etc)
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
		created => $args{created} // DateTime->now,
		data => $job,
		status => $args{status} // 'Pending',
		attempt => 1,
	};

	# TODO allow non-blocking enqueue
	my $id = $self->collection->insert($db_job);

	return $id;
}

sub monitor {
	my ($self, $id, $status, $callback) = @_;

	$status //= 'Complete';

	# args
	# - wait $queue $id, 'Status' => $callback

	if($callback) {
		# Non-blocking
		$self->log->debug("Waiting for $id on status $status in non-blocking mode");
		return Mojo::IOLoop->timer(0 => sub { $self->_monitor_nonblocking($id, $status, $callback) });
	} else {
		# Blocking
		$self->log->debug("Waiting for $id on status $status in blocking mode");
		return $self->_monitor_blocking($id, $status);
	}
}

sub _monitor_blocking {
	my ($self, $id, $status) = @_;

	while(1) {
		my $doc = $self->collection->find_one({'_id' => $id});
		$self->log->debug("Job found by Mango: " . ($doc ? 'Yes' : 'No'));

		if($doc && ((!ref($status) && $doc->{status} eq $status) || (ref($status) eq 'ARRAY' && $doc->{status} ~~ @$status))) {
			return 1;
		} else {
			$self->delay->wait;
		}
	}
}

sub _monitor_nonblocking {
	my ($self, $id, $status, $callback) = @_;

	$self->collection->find_one({'_id' => $id} => sub {
		my ($cursor, $err, $doc) = @_;
		$self->log->debug("Job found by Mango: " . ($doc ? 'Yes' : 'No'));
		
		if($doc && ((!ref($status) && $doc->{status} eq $status) || (ref($status) eq 'ARRAY' && $doc->{status} ~~ @$status))) {
			$self->log->debug("Status is $status");
			$self->delay->reset;
			$callback->($doc);
		} else {
			$self->log->debug("Job not found or status doesn't match");
			$self->delay->wait(sub {
				return unless Mojo::IOLoop->is_running;
				Mojo::IOLoop->timer(0 => sub { $self->_monitor_nonblocking($id, $status, $callback) });
			});
			return undef;
		}
	});
}

sub requeue {
	my ($self, $job) = @_;

	$job->{status} = 'Pending';
	my $id = $self->collection->update({'_id' => $job->{_id}}, $job, {upsert => 1});

	return $id;

	# TODO non-blocking
}

sub dequeue {
	my ($self, $id) = @_;

	# TODO option to not remove on dequeue?
	# TODO non-blocking

	$self->collection->remove({'_id' => $id});
}

sub get {
	my ($self, $id) = @_;

	# TODO non-blocking

	return $self->collection->find_one({'_id' => $id});
}

sub fetch {
	my ($self, $callback) = @_;

	$self->log->debug("In fetch");

	if($callback) {
		$self->log->debug("Fetching in non-blocking mode");
		return Mojo::IOLoop->timer(0 => sub { $self->_watch_nonblocking($callback, 1) });
	} else {
		$self->log->debug("Fetching in blocking mode");
		return $self->_watch_blocking(1);
	}
}

sub watch {
	my ($self, $callback) = @_;

	$self->log->debug("In watch");

	if($callback) {
		$self->log->debug("Watching in non-blocking mode");
		return Mojo::IOLoop->timer(0 => sub { $self->_watch_nonblocking($callback, 0) });
	} else {
		$self->log->debug("Watching in blocking mode");
		return $self->_watch_blocking(0);
	}
}

sub _watch_blocking {
	my ($self, $fetch) = @_;

	while(1) {
		my $doc = $self->collection->find_and_modify($self->get_options);
		$self->log->debug("Job found by Mango: " . ($doc ? 'Yes' : 'No'));

		if($doc) {
			return $doc;
		} else {
			last if $fetch;
			$self->delay->wait;
		}
	}
}

sub _watch_nonblocking {
	my ($self, $callback, $fetch) = @_;

	$self->collection->find_and_modify($self->get_options => sub {
		my ($cursor, $err, $doc) = @_;
		$self->log->debug("Job found by Mango: " . ($doc ? 'Yes' : 'No'));
		
		if($doc) {
			$self->delay->reset;
			$callback->($doc);
			return unless Mojo::IOLoop->is_running;
			return if $fetch;
			Mojo::IOLoop->timer(0 => sub { $self->_watch_nonblocking($callback) });
		} else {
			$self->delay->wait(sub {
				return unless Mojo::IOLoop->is_running;
				return if $fetch;
				Mojo::IOLoop->timer(0 => sub { $self->_watch_nonblocking($callback) });
			});
			return undef;
		}
	});
}

1;

=encoding utf8

=head1 NAME

MangoX::Queue - A MongoDB queue implementation using Mango

=head1 SYNOPSIS

	use Mango;
	use MangoX::Queue;

	my $mango = Mango->new("mongodb://localhost:27017");
	my $collection = $mango->db('my_db')->collection('my_queue');

	my $queue = MangoX::Queue->new(collection => $collection);

	# To add a basic job
	enqueue $queue 'some job name';
	$queue->enqueue('some job name');

	# To add a complex job
	enqueue $queue +{
		foo => 'bar'
	};
	$queue->enqueue({
		foo => 'bar'
	});

	# To set priority
	enqueue $queue priority => 2, 'job_name';
	$queue->enqueue(priority => 2, 'job_name');

	# To set created
	enqueue $queue created => DateTime->now, 'job_name';
	$queue->enqueue(created => DateTime->now, 'job_name');

	# To set status
	enqueue $queue status => 'Pending', 'job_name';
	$queue->enqueue(status => 'Pending', 'job_name');

	# To set multiple options
	enqueue $queue priority => 1, created => DateTime->now, 'job_name';
	$queue->enqueue(priority => 1, created => DateTime->now, 'job_name');

	# To wait for a job status change (non-blocking)
	my $id = enqueue $queue 'test';
	monitor $queue $id, 'Complete' => sub {
		# Job status is 'Complete'
	};

	# To wait on mutliple statuses (non-blocking)
	my $id = enqueue $queue 'test';
	monitor $queue $id, ['Complete', 'Failed'] => sub {
		# Job status is 'Complete' or 'Failed'
	};

	# To wait for a job status change (blocking)
	my $id = enqueue $queue 'test';
	monitor $queue $id, 'Complete';

	# To fetch a job (blocking)
	my $job = fetch $queue;
	my $job = $queue->fetch;

	# To fetch a job (non-blocking)
	fetch $queue sub {
		my ($job) = @_;
		# ...
	};
	$queue->fetch(sub {
		my ($job) = @_;
		# ...
	});

	# To get a job by id (currently blocking)
	my $id = enqueue $queue 'test';
	my $job = get $queue $id;

	# To requeue a job (currently blocking)
	my $id = enqueue $queue 'test';
	my $job = get $queue $id;
	requeue $queue $job;

	# To dequeue a job (currently blocking)
	my $id = enqueue $queue 'test';
	dequeue $queue $id;

	# To watch a queue (blocking)
	while (my $job = watch $queue) {
		# ...
	}
	while (my $job = $queue->watch) {
		# ...
	}

	# To watch a queue (non-blocking)
	watch $queue sub {
		my ($job) = @_;
		# ...
	};
	$queue->watch(sub{
		my ($job) = @_;
		# ...
	});

=head1 DESCRIPTION

L<MangoX::Queue> is a MongoDB backed queue implementation using L<Mango> to support
blocking and non-blocking queues.

L<MangoX::Queue> makes no attempt to handle the L<Mango> connection, database or
collection - pass in a collection to the constructor and L<MangoX::Queue> will
use it. The collection can be plain, capped or sharded.

=head1 ATTRIBUTES

L<MangoX::Queue> implements the following attributes.

=head2 delay

	my $delay = $queue->delay;
	$queue->delay(MangoX::Queue::Delay->new);

The L<MangoX::Queue::Delay> responsible for dynamically controlling the
delay between queue queries.

=head2 collection

    my $collection = $queue->collection;
    $queue->collection($mango->db('foo')->collection('bar'));

    my $queue = MangoX::Queue->new(collection => $collection);

The L<Mango::Collection> representing the MongoDB queue collection.

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

=head1 METHODS

L<MangoX::Queue> implements the following methods.

=head2 dequeue

	my $job = fetch $queue;
	dequeue $queue $job;

Dequeues a job. Currently removes it from the collection.

=head2 enqueue

	enqueue $queue 'job name';
	enqueue $queue [ 'some', 'data' ];
	enqueue $queue +{ foo => 'bar' };

	$queue->enqueue('job name');
	$queue->enqueue([ 'some', 'data' ]);
	$queue->enqueue({ foo => 'bar' });

Add an item to the queue.

Currently uses priority 1 with a job status of 'Pending'.

=head2 fetch

	# In blocking mode
	my $job = fetch $queue;
	my $job = $queue->fetch;

	# In non-blocking mode
	fetch $queue sub {
		my ($job) = @_;
		# ...
	};
	$queue->fetch(sub {
		my ($job) = @_;
		# ...
	});

Fetch a single job from the queue, returning undef if no jobs are available.

Currently sets job status to 'Retrieved'.

=head2 get

	my $job = get $queue $id;

Gets a job from the queue by ID. Doesn't change the job status.

=head2 get_options

	my $options = $queue->get_options;

Returns the L<Mango::Collection> options hash used by find_and_modify to
identify and update available queue items.

=head2 monitor

	# In blocking mode
	my $id = enqueue $queue 'test';
	monitor $queue $id, 'Complete'; # blocks until job is complete

	# In non-blocking mode
	my $id = enqueue $queue 'test';
	monitor $queue $id, 'Complete' => sub {
		# ...
	};

Wait for a job to enter a certain status.

=head2 requeue

	my $job = fetch $queue;
	requeue $queue $job;

Requeues a job. Sets the job status to 'Pending'.

=head2 watch

	# In blocking mode
	while(my $job = watch $queue) {
		# ...
	}
	while(my $job = $queue->watch) {
		# ...
	}

	# In non-blocking mode
	watch $queue sub {
		my ($job) = @_;
		# ...
	};
	$queue->watch(sub {
		my ($job) = @_;
		# ...
	});

Watches the queue for jobs, sleeping between queue checks using L<MangoX::Queue::Delay>.

Currently sets job status to 'Retrieved'.

=head1 SEE ALSO

L<Mojolicious>, L<Mango>

=cut