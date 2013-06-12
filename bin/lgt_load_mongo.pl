#!/usr/bin/perl

use strict;
use DBI;
use Getopt::Long qw(:config no_ignore_case no_auto_abbrev);
use Pod::Usage;
use File::Basename;
use MongoDB;
use GiTaxon;
use Data::Dumper;
$|++;

my %options = ();
my $results = GetOptions (
    \%options,
    'file_list=s',
    'db=s',
    'host=s',
    'ncbitax=s',
    'gitax=s',
    'dbhost=s',
    'taxondb=s',
    'taxoncoll=s',
    'overwrite=s',
    'idx_dir=s',
    'human:s',
    'dry_run:s',
    'update:s',
    'updatefield:s',
    'updatecol:s',
    'updatekey:s',
    'updatekeycol:s',
    'updatetype:s',
    'step:s',
    'chunk_size:s',
    'numeric:s',
    'metadata:s',
    'shard_column:s',
    'help|h') || pod2usage();

# display documentation
if( $options{'help'} ){
    pod2usage( {-exitval=>0, -verbose => 2, -output => \*STDOUT} );
}

$MongoDB::BSON::looks_like_number = 1;
my $CHUNK_SIZE = $options{chunk_size} ? $options{chunk_size} : 10000;
my $samtools = "/usr/local/bin/samtools";
my $mongo_conn = MongoDB::Connection->new(host => $options{host});
my $mongo_db = $mongo_conn->get_database($options{db});
my $admin = $mongo_conn->get_database('admin');
my $res = $admin->run_command({'isdbgrid' =>1});
my $shardcol = $options{shard_column} ? $options{shard_column} : 'clone_id';
my $mongo_coll = $mongo_db->get_collection('bwa_mapping');
if($res->{isdbgrid}) {
    print "Looks like we're sharded\n";
    print Dumper $admin->run_command({'enablesharding' => $options{db}});
    print Dumper $admin->run_command(['shardcollection' => $options{db}.'.bwa_mapping','key' => {$shardcol => 1},'unique'=> 1]);
}

my $gi2tax = undef;
if(!$options{human} && !$options{update} && $options{file_list}) {
    $gi2tax = &getGi2Tax();
}


my $seen_reads = {};
my $all_reads = {};
my $seen_this_file = {};
my $hits = {};
my $chunk = [];
my $counter = 0;
my $step = 0;
my $overlaps = {};
my $metadata = {};
my $metadata_types = {};

# Read in a metadata file if one is provided.
if($options{metadata}) {
    print "$options{file_list}\n";
    &process_metadata();
    if(!$options{file_list}) {
#        print STDERR "Here with".(Dumper($metadata))."\n";
#        foreach my $key (keys %$metadata) {
#            print STDERR "loading $key\n";
            # Changing this to an update which is effectively an upsert?
#            $mongo_coll->insert($metadata->{$key});            
#        }
        exit;
    }
}

if($options{human}) {
    print "Going to process human lines\n";
    open IN, "<$options{file_list}" or die "Couldn't open $options{file_list}\n";
    while(<IN>) {
        chomp;
        if($options{human}) {
            &process_huline($_);
        }
    }
}
elsif($options{update}) {
    open IN, "<$options{file_list}" or die "Couldn't open $options{file_list}\n";
    my $updatekey = $options{updatekey} ? $options{updatekey} : 'read';
    my $updatekeycol = defined $options{updatekeycol} ? $options{updatekeycol} : 0;

    while(<IN>) {
        my $line =$_;
        open IN2, "<$line" or die "Unable to open $line\n";
        print STDERR "Updating from $line\n";
        while(<IN2>) {
            my @fields = split(/\t/,$_);
            my $obj = {
                '$set' => {
                    $options{updatefield} => $fields[$options{updatecol}]
                }
            };
            if($options{updatetype} eq 'numeric') {
                $obj = {
                    '$set' => {
                        $options{updatefield} => $fields[$options{updatecol}] * 1.0
                    }
                };
            }
            elsif($options{updatetype} eq 'list') {
                my @val = split(/;/,$fields[$options{updatecol}]);
                $obj = {
                    '$set' => {
                        $options{updatefield} => \@val
                    }
                };
            }
            if($options{dry_run} && defined $fields[$options{updatecol}]) {
                print "$updatekey $fields[$updatekeycol]\n";
                print Dumper $obj;
            }
            else {
                if(defined $fields[$options{updatecol}]) {
                    $mongo_coll->update({$updatekey => $fields[$updatekeycol]},
                                        $obj,
                                        {'multiple'=>1});
                }
            }
        }
    }
}
elsif(defined($options{step})) {
    my $count = `wc -l $options{file_list}`;
    if(!($count*100)) {
        print STDERR "Couldn't get a line count $?\n";
    }
    $step = $options{step};
    print "Working on ".($step*$CHUNK_SIZE)." to ".($step*$CHUNK_SIZE+$CHUNK_SIZE)."\n";
    open IN, "<$options{file_list}" or die "Couldn't open $options{file_list}\n";
    my $cnter = 0;
    while(<IN>) {
        chomp;
        print join('',("\r",($cnter/$count*100)," done with step $step"));
        $cnter++;
#            print "Looking at $_ ".(keys %$all_reads)." for $step\n";
#        &process_line4($_);
         &process_line5($_);
    }
    print "\nLoading $_ ".(keys %$all_reads)." for $step\n";
    foreach my $key (keys %$all_reads) {
        print STDERR "loading $key\n";
        # Changing this to an update which is effectively an upsert?
        $mongo_coll->insert($all_reads->{$key});
    }
}
else {
    &loop_over_list();
}

if(0) {
foreach my $key (keys %$all_reads) {
    push(@$chunk, $all_reads->{$key});
    if(@$chunk >= $CHUNK_SIZE) {
        print "Inserting a chunk\n";
        if(!$options{dry_run}) {
            $mongo_coll->batch_insert($chunk);
        }
        $chunk = [];
    }
}
}
sub process_huline {
    my $line = shift;
    print "Processing $line\n"; 
    my $handle;
    if($line =~ /.bam/) {
        my $tries = 50;
        my $t =0;
        while($t < $tries && !$handle) {
            open($handle, "-|", "$samtools view $line") or print STDERR "Couldn't run samtools on $line\n";
            $t++;
        }
        if(!$handle) {
            die "Couldn't run samtools on $line\n";
        }
    }
    else {
        open $handle, "<$line" or die "Unable to open $line\n";
    }
    while (<$handle>) {
        chomp;
        next if /^@/;
        my @fields = split();

        my $flag = &parseFlag($fields[1]);
        if($flag->{query_mapped}) {
            $fields[0] =~ /^([^.]+)\..*/;
            my $length = length $fields[9];
            my $obj = {
                '$set' => {
                    'hu_ref'=> $fields[2],
                    'hu_start'=> $fields[3],
                    'hu_stop' => $fields[3]+$length
                }
            };
            if(!$options{dry_run}) {
                $mongo_coll->update({'read' => $fields[0]},
                                     $obj,
                                    {'multiple'=>1});
            }
            else {
                print "Was going to load:\n";
                print Dumper $obj;
            }
        }
    }
    close $handle;
}


sub process_metadata {

    print STDERR "Reading in the metadata file\n";
    open IN, "<$options{metadata}" or die "Unable to open metadata file $options{metadata}\n";
    my $hl = <IN>;
    chomp $hl;
    my @hlist = split(/\t/,$hl);
    my @head;
    foreach my $h (@hlist) {
        if($h =~ /(\S+):(\S+)/) {        
            $metadata_types->{$1} = $2;
            push(@head, $1);
        }
        else {
            push(@head, $h);
        }
    }
    
#    print STDERR @head;
#    print STDERR "\n";
    while(<IN>) {
        chomp;
        my @fields = split(/\t/,$_);
        $metadata->{$fields[1]} = {};
        for(my $i = 0; $i < @head; $i++) {
            my $type = $metadata_types->{$head[$i]};
            my $value = $fields[$i];
            if($type eq 'numeric') {
                $value => $fields[$i] * 1.0;
            }
            elsif($type eq 'list') {
                my @val = split(/;/,$fields[$i]);
                $value =  \@val
            }
            #print "$fields[0] $head[$i] $value\n";
            $metadata->{$fields[1]}->{$head[$i]} = $value;
        }
        if(!$options{file_list}) {
            print STDERR Dumper $metadata->{$fields[1]};
            $mongo_coll->insert($metadata->{$fields[1]});
            $metadata = {};  
        }
    }
    print STDERR "Done reading the metadata file\n";
}

sub loop_over_list {
    my $count = `wc -l $options{file_list}`;

    while(keys %$all_reads >0 || !$step) {
        $all_reads = {};
        my $cnter = 0;
        open IN, "<$options{file_list}" or die "Couldn't open $options{file_list}\n";
        while(<IN>) {
            chomp;
            print join('',("\r",($cnter/$count*100)," done with step $step with ",scalar keys %$all_reads,' ',scalar keys %$seen_reads,"#"));
            $cnter++;
#            print "Looking at $_ ".(keys %$all_reads)." for $step\n";
#            &process_line4($_);
            &process_line5($_);
        }
        print "\nLoading $_ ".(keys %$all_reads)." for $step\n";
        foreach my $key (keys %$all_reads) {
            $mongo_coll->insert($all_reads->{$key});            
        }
        $step++;
        close IN;
    }
}

sub read_overlap_file {
    my $file = shift;

    open OLAP, "<$file" or die;

    while(<OLAP>) {
        my @fields = split(/\t/,$_);
        $overlaps->{$fields[0]} = {'type' => $fields[1],
                                   'product' => $fields[2]};
    }
    close OLAP;
}

sub process_line5 {
    my $line = shift;

    my $handle;    
    
    my $presplit = 0;

    my($name,$path,$suff) = fileparse($line,('.sam','.bam'));

    $overlaps = {};
    my $overlap_file = `ls $path/$name*\_features.out`;
    chomp $overlap_file;
    if(-e $overlap_file) {
        print "Reading $overlap_file\n";
        &read_overlap_file($overlap_file);
    }

    print STDERR "Checking for $path/split/$name\_$step$suff\n"; 
    if( -e "$path/split/$name\_$step$suff") { 
        print STDERR "Looks like we are pre-split\n";
        $line = "$path/split/$name\_$step$suff";
        ($name,$path,$suff) = fileparse($line,('.sam','.bam'));
        $presplit = 1;
    }

    if($line =~ /.bam$/) {
        my $tries = 50;
        my $t =0;
        my $res = 0;
        while($t < $tries && !$res) {
            $res = open($handle, "-|", "$samtools view $line");
            if(!$res) {
                my @lines = <$handle>;
                print STDERR @lines;
                print STDERR $?;
                print STDERR "Couldn't run samtools on $line $res $!\n";
                sleep 30;
            }
            $t++;
        }
        if(!$res) {
            print STDERR `$samtools view $line`;
            die "Couldn't run samtools on $line $res $!\n";
        }
        print "opened $line successfully\n";
        #open($handle, "-|", "$samtools view $line")  or die "Couldn't run samtools on $line\n";
    }
    else {
        open $handle, "<$line" or die "Unable to open $line\n";
    }
    $seen_this_file ={};
    my $count = 0;

    # Loop till we're done.
    my $end = $CHUNK_SIZE;
    $count = 0;
    my $l;

    # If we have presplit the files we'll process the whole thing
    if($presplit) {
        while ($l = <$handle>) {
            chomp $l;
            # Don't count @seq lines
            if($l =~ /^@/) {
                next;
            }
            $count++;
            &process_sam_line($l);
        }
    }
    # If we aren't pre-split we'll jump to the position in the file
    # and go from there.
    else {
        print STDERR "Jumping to line $step\n";
        &jump_to_line($handle,$step);
        while ($count < $end && ($l = <$handle>)) {
            chomp $l;
            # Don't count @seq lines
            if($l =~ /^@/) {
                next;
            }
            $count++;
            &process_sam_line($l);
        }
    }
    close $handle;
    print STDERR scalar keys (%$seen_reads) . " reads with hits total\n";
    print STDERR scalar keys (%$seen_this_file) . " reads with hits this file\n";
    print STDERR scalar keys (%$all_reads) . " reads total\n";
}
sub process_line4 {
    my $line = shift;

    my $handle;
    if($line =~ /.bam$/) {
        open($handle, "-|", "$samtools view $line");
    }
    else {
        open $handle, "<$line" or die "Unable to open $line\n";
    }
    my $seen_this_file ={};
    while (<$handle>) {
        chomp;
        next if /^@/;
        my @fields = split();
        my $flag = &parseFlag($fields[1]);

        if($flag->{query_mapped}) {
            $fields[0] =~ /^([^.]+)\..*/;
            my $length = length $fields[9];
            my $sample = $1;
            my $obj = {
                'ref'=> $fields[2],
                'start'=> $fields[3],
                'stop' => $fields[3]+$length
            };
            if($fields[2] =~ /^gi/) {
                my $tax = $gi2tax->getTaxon($fields[2]);
#                print "@fields\n";
                #print $tax->{name}."\n";
                $tax->{name} =~ /^(\w+) /;
                $obj->{genus} = $1;
                $obj->{scientific} = $tax->{name};
                $obj->{taxon_id} = $tax->{taxon_id};
            }
            else {
                $obj->{genus} = 'Homo';
                $obj->{scientific} = 'Homo sapien';
            }
            # This is one of our reads
            if($all_reads->{$fields[0]}) {
                push(@{$all_reads->{$fields[0]}->{hits}},$obj);
            }
            # This isn't one of our reads but we don't have 
            # all of them yet. Also, make sure we are in the right range of reads.
            elsif((keys %$all_reads < $CHUNK_SIZE) && 
                (keys %$seen_reads >= $CHUNK_SIZE*$step)
                && !$seen_reads->{$fields[0]}){
                $seen_reads->{$fields[0]} = 1;
                $all_reads->{$fields[0]} = {
                    'read' => $fields[0],
                    'sample_id' => $sample,
                    'hits' => [$obj]
                };
            }
        }       
    }
    close $handle;
}


sub getGi2Tax {
    my $ncbitax = $options{ncbitax} ? $options{ncbitax} : '/local/db/repository/ncbi/blast/20120414_001321/taxonomy/taxdump/';
    my $gi2tax = $options{gitax} ? $options{gitax} : '/local/db/repository/ncbi/blast/20120414_001321/taxonomy/gi_taxid_nucl.dmp';
    my $dbhost = $options{dbhost} ? $options{dbhost} : 'tettelin-lx.igs.umaryland.edu';
    my $taxondb = $options{taxondb} ? $options{taxondb} : 'gi2taxon';
    my $idx_dir = $options{idx_dir} ? $options{idx_dir} : $ncbitax;
    my $taxoncoll = $options{taxoncoll};
    if(!$taxoncoll) {
        $ncbitax =~ /(\d+\_\d+)/;
        my $date = $1;
        if($gi2tax =~ /nuc/) {
            $taxoncoll = "gi2taxonnuc_$date";
        }
        else {
            $taxoncoll = "gi2taxonprot_$date";
        }
        
    }
    
    my $idx_dir = $options{idx_dir};
    
    if(!$idx_dir && -e "$ncbitax/names") {
        $idx_dir = $ncbitax;
    }
    else {
#        $idx_dir='/tmp/';
    }
    
    
    
    my $gi2tax = GiTaxon->new(
        {'nodes' => "$ncbitax/nodes.dmp",
         'names' => "$ncbitax/names.dmp",
         'gi2tax' => $gi2tax,
         'chunk_size' => 10000,
         'idx_dir' => $idx_dir,
         'host' => $dbhost,
         'gi_db' => $taxondb,
         'gi_coll' => $taxoncoll
        });
}

sub jump_to_line {
    my $handle = shift;
    my $count = 0;
    my $start = $step*$CHUNK_SIZE;
    while ($count < $start) {
        my $line = <$handle>;
        if ($line !~ /^@/) {
            $count++;
        }
    }
    return $count;
}

sub find_lca {
    my $lineages = shift;

    # prime it
    my @lca = split(';', $lineages->[0]);

    foreach my $l (@$lineages) {
        my $newlca = [];
        my @lineage = split(';',$l);
        for( my $i = 0; $i < @lineage;$i++) {
            if($lca[$i] eq $lineage[$i]) {
                push(@$newlca, $lineage[$i]);
            }
            else {
                last;
            }
        }
        @lca = @$newlca;
    }
    #print STDERR join(";",@lca);
    #print STDERR "\n";
    return join(';',@lca);
}

sub dec2bin {
    my $str = unpack("B32", pack("N", shift));
    $str =~ s/^0+(?=\d)//;   # otherwise you'll get leading zeros
    return $str;
}

sub parseFlag {
    my $flag = shift;
    my $rawbin = dec2bin($flag);
    my $rev = scalar $rawbin;
    if($rev eq $rawbin) {
        #    print "ERROR $rev $rawbin\n";
    }
    my $bin = sprintf("%011d", $rev);
    my $final_bin = reverse $bin;
    my $prop = substr($final_bin, 1, 1);
    my $qmap = substr($final_bin, 2, 1);
    my $mmap = substr($final_bin, 3, 1);
    my $qstrand = substr($final_bin, 4, 1);
    my $mstrand = substr($final_bin, 5, 1);

    return {
        'query_mapped' => !$qmap,
        'mate_mapped' => !$mmap
    };
}

sub process_sam_line {

    my $l = shift;
    my @fields = split(/\t/,$l);


    my $sample;
    # If we have never seen this read before...
    if(!$all_reads->{$fields[0]}) {
        # HACK here - assuming the ID's are of the format SAMPLEID.READID
        $fields[0] =~ /^([^.]+)\..*/;
        $sample = $1;
        if($metadata->{$fields[0]}) {
            $all_reads->{$fields[0]} = $metadata->{$fields[0]};
            $all_reads->{$fields[0]}->{sample_id} = $sample;
            $all_reads->{$fields[0]}->{read} = $fields[0];
            $all_reads->{$fields[0]}->{hits} = [];
            $all_reads->{$fields[0]}->{lca} = undef;
#            print STDERR Dumper $all_reads->{$fields[0]};
        }
        else {
            $all_reads->{$fields[0]} = {
                'read' => $fields[0],
                'sample_id' => $sample,
                'hits' => [],
                'lca' => undef
            };
        }
#        $seen_reads->{$fields[0]} = 1;
    }

    my $flag = &parseFlag($fields[1]);


    
    # Check if we are mapped
    if($flag->{query_mapped}) {
#        $seen_reads->{$fields[0]} = 2;
        # If it's one of our reads we'll say we saw it.
        $seen_this_file->{$fields[0]} = 1;
        $seen_reads->{$fields[0]} = 1;
        my $length = length $fields[9];


        # Initiate a hit object
        my $obj = {
            'ref'=> $fields[2],
            'start'=> $fields[3],
            'stop' => $fields[3]+$length
        };
        
        # Check for overlaps
        if($overlaps->{$fields[0]}) {
            $obj->{'feat_type'} = $overlaps->{$fields[0]}->{type};
            $obj->{'feat_product'} = $overlaps->{$fields[0]}->{product};
        }

        # Pull the taxonomic info
        my $tax;
        if($fields[2] =~ /^gi/) {
            $tax = $gi2tax->getTaxon($fields[2]);
#                print "@fields\n";
            #print $tax->{name}."\n";
            $tax->{name} =~ /^(\w+) /;
            $obj->{genus} = $1;
            $obj->{scientific} = $tax->{name};
            $obj->{taxon_id} = $tax->{taxon_id};
        }

        # HACK - if it doesn't start with gi we assume it is 
        # from human.
        else {
            $obj->{genus} = 'Homo';
            $obj->{scientific} = 'Homo sapien';
        }

        # We've seen this one before so we'll apply the lineage to the
        # lca and add the hit object to the hits array
        if($all_reads->{$fields[0]}) {
            if($tax->{lineage}) {

                # Default the lca to the lineage of this hit.
                my $lca = $tax->{lineage};

                # If there was a previously defined lca we'll calculate the new one
                if($all_reads->{$fields[0]}->{'lca'}) {
                    $lca = &find_lca([$tax->{lineage},join(';',@{$all_reads->{$fields[0]}->{'lca'}})]);
                }

                # Make it a list
                if(!$lca) {
                    print STDERR "Had no lca:\n$lca\n$tax->{lineage}\n".join(';',@{$all_reads->{$fields[0]}->{'lca'}})."\n";
                }
                my @lca = split(';',$lca);
                $all_reads->{$fields[0]}->{'lca'} = \@lca;
            }
            push(@{$all_reads->{$fields[0]}->{hits}},$obj);
        }
        # We haven't seen this one before so initalize all of the values in the mate
        # object and set the lca to the lineage of this hit.
        else {
            print STDERR "Shouldn't be here!!\n";
 #           my @lca = split(';',$tax->{lineage});
            
 #           $all_reads->{$fields[0]} = {
 #               'read' => $fields[0],
 #               'sample_id' => $sample,
 #               'hits' => [$obj],
 #               'lca' => \@lca
 #           };
        }
    }
}
