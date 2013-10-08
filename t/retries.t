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

$queue->timeout(1);

enqueue $queue 'test28451289';
my $job = fetch $queue;

isnt($job, undef, 'Got job from queue');
is($job->{data}, 'test28451289', 'Got the right job');

$job = fetch $queue;
is($job, undef, 'No job left in queue');

sleep 2;

$job = fetch $queue;
isnt($job, undef, 'Got job from queue');
is($job->{data}, 'test28451289', 'Got the right job');

done_testing;