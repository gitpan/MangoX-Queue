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

test_nonblocking_watch();
test_blocking_watch();

sub test_nonblocking_watch {
	enqueue $queue 'test';

	my $happened = 0;

	watch $queue sub {
		my ($job) = @_;

		$happened++;
		ok(1, 'Found job ' . $happened . ' in non-blocking watch');

		if($happened == 2) {
			Mojo::IOLoop->stop;
			return;
		}

		Mojo::IOLoop->timer(1 => sub {
			enqueue $queue 'another test';
		});
	};

	is($happened, 0, 'Non-blocking watch successful');

	Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
}

sub test_blocking_watch {
	enqueue $queue 'test';

	while(my $item = watch $queue) {
		ok(1, 'Found job in blocking watch');
		last;
	}
}

done_testing;