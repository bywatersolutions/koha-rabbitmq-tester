#!/usr/bin/perl

use Modern::Perl;
use feature 'say';

use Getopt::Long;
use JSON qw( encode_json );
use Net::Stomp;
use Pod::Usage;
use Try::Tiny;

use C4::Context;
use Koha::BackgroundJob;
use Koha::DateUtils qw( dt_from_string );
use Koha::Patrons;

my $p_hostname;
my $p_port;
my $login;
my $passcode;
my $vhost;
my $verbose;
my $help;
my $man;

GetOptions(
    "hostname=s" => \$p_hostname,
    "port=s"     => \$p_port,
    "login=s"    => \$login,
    "passcode=s" => \$passcode,
    "vhost=s"    => \$vhost,
    "v|verbose+" => \$verbose,
    "h|help"     => \$help,
    "m|man"      => \$man,
) or pod2usage(2);
pod2usage(1) if $help;
pod2usage( -exitval => 0, -verbose => 2 ) if $man;

=head1 NAME

RabbitMQ Tester - Mock sending Koha messages to RabbitMQ via Stomp

=head1 SYNOPSIS

rabbitmq_tester.pl [options]

 Options:
   -help -h         brief help message
   -man             full documentation
   -hostname        override Koha's hostname for rabbitmq server
   -port            override Koha's port for rabbitmq server
   -login           override Koha's username for rabbitmq server
   -passcode        override Koha's password for rabbitmq server
   -vhost           override Koha's vhost for rabbitmq server

=head1 OPTIONS

=over 8

=item B<-help>

Print a brief help message and exits.

=item B<-man>

Prints the manual page and exits.

=item B<-hostname>

Override's the default hostname that Koha would have used.

=item B<-port>

Override's the default port that Koha would have used.

=item B<-login>

Override's the default login that Koha would have used.

=item B<-passcode>

Override's the default passcode that Koha would have used.

=item B<-vhost>

Override's the default vhost that Koha would have used.
This setting has no affect that we know of at the time of this writing.

=back

=head1 DESCRIPTION

B<This program> is meant to assist in debugging RabbitMQ connection issues.

=cut

my $hostname    = 'localhost';
my $port        = '61613';
my $config      = C4::Context->config('message_broker');
my $credentials = {
    login    => 'guest',
    passcode => 'guest',
};

if ($config) {
    $hostname                = $config->{hostname} if $config->{hostname};
    $port                    = $config->{port}     if $config->{port};
    $credentials->{login}    = $config->{username} if $config->{username};
    $credentials->{passcode} = $config->{password} if $config->{password};
    $credentials->{host}     = $config->{vhost}    if $config->{vhost};
}

$hostname                = $p_hostname if $p_hostname;
$port                    = $p_port     if $p_port;
$credentials->{login}    = $login      if $login;
$credentials->{passcode} = $passcode   if $passcode;
$credentials->{host}     = $vhost      if $vhost;

say "Using the following parameters:
Hostname: $hostname
    Port: $port
   Login: $credentials->{login}
Passcode: $credentials->{passcode}
    Host: $credentials->{host}
" if $verbose;

my $stomp = Net::Stomp->new( { hostname => $hostname, port => $port } );
$stomp->connect($credentials);

my $job_type = 'batch_authority_record_modification';
my $job_size = 1;
my $job_args = {};

my $borrowernumber = Koha::Patrons->search()->next()->id;
my $json_args      = encode_json $job_args;

my $job = Koha::BackgroundJob->new(
    {
        status         => 'new',
        type           => $job_type,
        size           => $job_size,
        data           => $json_args,
        enqueued_on    => dt_from_string,
        borrowernumber => $borrowernumber,
    }
)->store;

$job_args->{job_id} = $job->id;
$json_args = encode_json $job_args;

say "Background job ID: " . $job->id;

say "Connecting to RabbitMQ server...";
my $conn = $job->connect;

my $namespace = C4::Context->config('memcached_namespace');

say "Using namespace: $namespace";
say "Using queue: " . sprintf( "/queue/%s-%s", $namespace, $job_type );

print "Sending with receipt... ";
my $ret = $conn->send_with_receipt(
    {
        destination => sprintf( "/queue/%s-%s", $namespace, $job_type ),
        body        => $json_args
    }
);
say $ret ? "Succeeded!" : "Failed!";
