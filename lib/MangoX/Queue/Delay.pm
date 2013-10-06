package MangoX::Queue::Delay;

use Mojo::Base -base;
use Mojo::Log;

has start     => sub { $ENV{MANGOX_QUEUE_DELAY_START}     // 0.1  };
has current   => sub { $ENV{MANGOX_QUEUE_DELAY_START}     // 0.1  };
has increment => sub { $ENV{MANGOX_QUEUE_DELAY_INCREMENT} // 0.1  };
has maximum   => sub { $ENV{MANGOX_QUEUE_DELAY_MAXIMUM}   // 10   };

has log => sub { Mojo::Log->new->level('error') };

sub reset {
	my ($self) = @_;
	
	$self->log->debug("Reset delay to " . $self->start);

	$self->current($self->start);
}

sub wait {
	my ($self, $callback) = @_;

	my $delay = $self->current;
	$self->log->debug("Current delay is " . $delay);

	my $incremented = $delay + $self->increment;
	$self->log->debug("New delay is " . $incremented);

	if($incremented > $self->maximum) {
		$self->log->debug("Limiting delay to maximum " . $self->maximum);
		$incremented = $self->maximum;
	}

	$self->current($incremented);

	if($callback) {
		$self->log->debug("Non-blocking delay for " . $delay);
		Mojo::IOLoop->timer($delay => $callback);
	} else {
		$self->log->debug("Sleeping for " . $delay);
		sleep $delay;
	}

	return $delay;
}

1;