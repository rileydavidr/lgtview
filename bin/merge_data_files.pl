#!/usr/bin/perl

use strict;
use Getopt::Long qw(:config no_ignore_case no_auto_abbrev);
use Pod::Usage;
use File::Basename;
use Data::Dumper;
$|++;

my %options = ();
my $results = GetOptions (
    \%options,
    'file1:s',
    'filelist1:s',
    'file1_index=s',
    'file1_uniq_index:s',
    'file1_headers=s',
    'file2:s',
    'filelist2:s',
    'file2_index=s',
    'file2_headers=s',
    'assume_unique=s',
    'help|h') || pod2usage();

my $SAMTOOLS_BIN="/usr/local/bin/samtools";

my $complete_header = [];


# First deal with the first file

my @file1;

if($options{file1}) {
    push(@file1,$options{file1});

}
if($options{filelist1}) {
    open IN, "<$options{filelist1}" or die "Unable to open filelist1 $options{filelist1}\n";
    while(<IN>) {
        chomp;
        push(@file1,$_);
    }
    close IN;
}

my $data ={};
my @head1 = $options{file1_headers} ? split(/,/, $options{file1_headers}) : ();
my $uniq_index = defined($options{file1_uniq_index}) ? $options{file1_uniq_index} : undef;
print STDERR "Unique index was $uniq_index\n";
foreach my $file (@file1) {
    &read_file($file,\@head1,$options{file1_index},$uniq_index,$data);
}

push(@$complete_header, @head1);
print STDERR "@$complete_header\n";
# Now deal with the second file

my @file2;

if($options{file2}) {
    push(@file2,$options{file2});

}
if($options{filelist2}) {
    open IN, "<$options{filelist2}" or die "Unable to open filelist2 $options{filelist2}\n";
    while(<IN>) {
        chomp;
        push(@file2,$_);
    }
    close IN;
}

my @head2 = $options{file2_headers} ? split(/,/, $options{file2_headers}) :();
foreach my $file (@file2) {
    &read_file($file,\@head2,$options{file2_index},undef,$data);
}

push(@$complete_header,@head2);


print join("\t", @$complete_header);
print "\n";

if(defined($options{file1_uniq_index})) {
    foreach my $key (keys %$data) {
        foreach my $key2 (keys %{$data->{$key}}) {
            my @f;
            foreach my $head (@$complete_header) {
                push(@f,$data->{$key}->{$key2}->{$head});
            }
            print join("\t",@f);
            print "\n";
        }
    }
}
else {
    foreach my $key (keys %$data) {
        my @f;
        foreach my $head (@$complete_header) {
            push(@f,$data->{$key}->{$head});
        }
        print join("\t",@f);
        print "\n";
        
    }
}


sub read_file {
    my $f = shift;
    my $head = shift;
    my $key = shift;
    my $uniq_key = shift;
    my $hash = shift;
    my $h;
    if($f =~ /.bam$/) {
        open($h, "-|", "$SAMTOOLS_BIN view $f") or die "Couldn't open $f\n";
    }
    else {
        open $h, "<$f" or die "Unable to open $f\n";
    }
    if(! scalar @$head) {
        my $line = <$h>;
        print STDERR "Needed the head $line\n";
        @$head = split(/\t/,$line);
        chomp $head->[-1];
    }
    while(my $line = <$h>) {

        my @fields = split(/\t/, $line);
        chomp $fields[-1];

        # If we have a key that is uniq that is not our index key go in here
        if(defined($uniq_key)) {
            if(!$hash->{$fields[$key]}) {
                $hash->{$fields[$key]} = {$fields[$uniq_key] => {}};
            }
            for(my $i = 0; $i < @$head; $i++) {
                $hash->{$fields[$key]}->{$fields[$uniq_key]}->{$head->[$i]} = $fields[$i];
            }
        }
        # If we were passed a uniq index but this file doesn't have it then we'll go in here
        elsif(defined($options{file1_uniq_index})) {
            # If we don't have a uniq key besides our index key go in here
            if(!$hash->{$fields[$key]}) {
                print STDERR "Don't have a uniq index for $fields[$key]\n";
                next;
#                $hash->{$fields[$key]} = {};
            }
            foreach my $k (keys %{$hash->{$fields[$key]}}) {
                
                for(my $i = 0; $i < @$head; $i++) {
                    $hash->{$fields[$key]}->{$k}->{$head->[$i]} = $fields[$i];
                }
            }
        }
        # If we weren't passed a uniq index go in here
        else {
            if(!$hash->{$fields[$key]}) {
                $hash->{$fields[$key]} = {};
            }
            #print STDERR "$fields[$key]\t@$head\n";
            for(my $i = 0; $i < @$head; $i++) {
                $hash->{$fields[$key]}->{$head->[$i]} = $fields[$i];
            }
        }
    }
}
