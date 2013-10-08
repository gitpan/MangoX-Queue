#!/usr/bin/env perl

use strict;
use warnings;

use Mango;
use MangoX::Queue;

use Test::More;

my $mango = Mango->new('mongodb://localhost:27017');
my $collection = $mango->db('test')->collection('mangox_queue_test');
$collection->remove;

my $queue = MangoX::Queue->new(collection => $collection);

test_nonblocking_consume();
test_blocking_consume();
test_custom_consume();

sub test_nonblocking_consume {
	enqueue $queue '82365';

	my $happened = 0;

	my $consumer_id;
	$consumer_id = consume $queue sub {
		my ($job) = @_;

		$happened++;
		if($happened == 1) {
			is($job->{data}, '82365', 'Found job 82365 in non-blocking consume');
			Mojo::IOLoop->timer(1 => sub {
				enqueue $queue '29345';
			});
		} elsif ($happened == 2) {
			is($job->{data}, '29345', 'Found job 29345 in non-blocking consume');
			release $queue $consumer_id;
			Mojo::IOLoop->stop;
		} else {
			use Data::Dumper; print Dumper $job;
			fail('Queue consumed too many items');
		}
	};

	is($happened, 0, 'Non-blocking consume successful');

	Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
}

sub test_blocking_consume {
	enqueue $queue 'test';

	while(my $item = consume $queue) {
		ok(1, 'Found job in blocking consume');
		last;
	}
}

sub test_custom_consume {
	$collection->remove;

	my $id = enqueue $queue 'custom consume test';

	my $happened = 0;

	my $consumer_id;
	$consumer_id = consume $queue status => 'Failed', sub {
		my ($job) = @_;

		isnt($job, undef, 'Found failed job in non-blocking custom consume');

		release $queue $consumer_id;
		Mojo::IOLoop->stop;
		return;
	};

	is($happened, 0, 'Non-blocking consume successful');

	Mojo::IOLoop->timer(1 => sub {
		my $job = get $queue $id;
		$job->{status} = 'Failed';
		update $queue $job;
	});

	Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
}

done_testing;