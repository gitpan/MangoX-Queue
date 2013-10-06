#!perl -w

###############################################################################
##                                                                           ##
##    Copyright (c) 2013 Ian Kent                                            ##
##    All rights reserved.                                                   ##
##                                                                           ##
##    This package is free software; you can redistribute it                 ##
##    and/or modify it under the same terms as Perl itself.                  ##
##                                                                           ##
###############################################################################

use strict;
use ExtUtils::MakeMaker;

BEGIN
{
    eval { require Config_m; }; # ExtUtils::FakeConfig (+ ActivePerl)
    eval { require Config;   } # Everyone else
    if ($@);
}

WriteMakefile(
    'NAME'          => 'MangoX::Queue',
    'VERSION_FROM'  => 'lib/MangoX/Queue.pm',
    'ABSTRACT_FROM' => 'lib/MangoX/Queue.pm',
    'LICENSE'       => 'perl',
    'AUTHOR'        => 'Ian Kent <iankent@cpan.org>',
    'PREREQ_PM'     => {
                           'Mango' => '0.13'
                       },
    'dist'          => { COMPRESS => "gzip -9", SUFFIX => "gz" },
#   for ActivePerl:
       ($] >= 5.005 && $^O eq 'MSWin32' && $Config::Config{'archname'} =~ /-object\b/i ?
               ('CAPI'         => 'TRUE') : ())
);

__END__
