#!/usr/bin/perl -w
use strict;
use MongoDB;
use MongoDB::Code;
use CGI;
use File::Basename;
use Data::Dumper;
use JSON;
use Digest::MD5 qw(md5_hex);

my $cgi = CGI->new;

my $host = $cgi->param('host');
my $db = $cgi->param('db');
my $list = $cgi->param('coll');
my $criteria = $cgi->param('criteria');
my $cond = $cgi->param('cond');
my $condfield = $cgi->param('condfield');
my $group = $cgi->param('group');
my $start = $cgi->param('start');
my $limit = $cgi->param('limit');

my $filter_limit = $cgi->param('flimit');

my $mongo_conn = MongoDB::Connection->new(host => $host);
$mongo_conn->query_timeout(-1);
my $mongo_db = $mongo_conn->get_database($db);
my $mongo_coll = $mongo_db->get_collection('bwa_mapping');
my $outputcollname = "$criteria\_mappings";
my $outputcoll = $mongo_db->get_collection($outputcollname);
my $result;

my $json = JSON->new;
$json->allow_blessed;
$json->convert_blessed;

if(!$outputcoll->find_one() && (!$cond || !$condfield)) {
    &runMapReduce();
}

if($criteria =~ /\./) {
    &runMapReduce2();
    &pullFromColl();
}

elsif($criteria && $cond) {
    &runMapReduce3();
    &pullFromColl();
}
elsif($criteria) {
    &pullFromColl();
}
else {
    &pullFromColl2();
}







#my $result = $mongo_db->run_command($cmd);

#my $rescoll = $result;
print "Content-type: text/plain\n\n";
print $json->encode($result);

sub pullFromColl2 {
    my $condhash = {};
    if($cond) {
        $condhash = from_json($cond);
    }
    my $cursor = $mongo_coll->query($condhash);
#    my $cursor = $mongo_coll->query($condhash)->limit($limit)->skip($start)->fields({'read'=>1});
    my $total = $cursor->count();
    my $limitcursor = $cursor->limit($limit)->skip($start)->fields({'read'=>1});
    my @res = $limitcursor->all();
#    my @res = $cursor->skip($start)->limit($limit)->all();
    $result = {'total'=> $total,'retval' => \@res};
}

sub pullFromColl {

    my $cursor = $outputcoll->find({});
    $cursor->sort({'value.count' =>1});
    my @res = $cursor->all();
    my @retval;
    my $len = scalar @res;
    my $min = 10;
    my $other = {'_id' => 'Other', 'count' => 0};
    
    my $total = 0;
    if($len > 200) {
        map {$total += $_->{value}->{count}; }@res;
    } 

    foreach my $ret (@res) {
        my $id = $ret->{'_id'} ? $ret->{'_id'} : 'Unknown';
        if($len <= 200 || ($len > 200 && $ret->{value}->{count}/$total >= .001)) {
        push(@retval, {'_id' => $id,
                       'count' => $ret->{value}->{count}});
        }
        else {
            $other->{count} += $ret->{value}->{count};
        }
    }
    if($other->{count}) {
        push(@retval, $other);
    }
#   my @srted_vals = sort {$a->{'_id'} <=> $b->{'_id'}} @res;
    $result = {'retval' => \@retval};
}

sub runMapReduce {

    my $map = <<MAP;
    function() {
        emit(this.$criteria, {count:1});
    }
MAP
        
    my $reduce = <<RED;
    function(key,values) {
        var result = {count:0.0};
        values.forEach(function(value) {
            result.count += value.count;
                       });
        return result;
    }
RED
        
    my $cmd = Tie::IxHash->new("mapreduce" => "bwa_mapping",
                               "map" => $map,
                               "reduce" => $reduce,
                               "out" => "$criteria\_mappings");
                               
    $mongo_db->run_command($cmd);

}

sub runMapReduce2 {

    my ($list,$val) = split(/\./,$criteria);
    my $scond = $cond ? from_json($cond) : undef;
    my $mapconds = [];
    my $otherconds = {};
    if($cond) {
        map {
            my $key = $_;
            if($key =~ /$list/) {
                $key =~ s/$list\.//;
                my $val = $scond->{$_};
                my $noteq = JSON::false;
                if(ref $scond->{$_} eq 'HASH') {
                    my @keys = keys %{$scond->{$_}};
                    $val = $scond->{$_}->{$keys[0]};
                    if($keys[0] eq '$ne') {
                        $noteq = JSON::true;
                    }
                }
                push(@$mapconds, {
                    'key' => $key,
                    'value' => $val,
                    'noteq' => $noteq
                });
            }
            else {
                $otherconds->{$_} = $scond->{$_};
            }
        } keys %$scond;
    }
    my $mapcondsjson = $json->encode($mapconds);
    my $map = <<MAP;
    function() {
        var conds = $mapcondsjson;
        var thiselm = this;
        var seen = {};
        var goods = [];
        var ggood = true;
        this.$list.forEach(
            function(h) {
                var good = true;
                if(conds.length > 0 && !seen[h.$val]) {
                    conds.forEach(
                        function(c) {
                            if(!c.noteq && h[c.key] != c.value) {                            
                                good = false;
                            }
                            else if(c.noteq && h[c.key] == c.value) {                            
                                good = false;
                                ggood = false;
                            }

                    });
                }
                if(good && !seen[h.$val]) {
                    goods.push(h.$val);
                    seen[h.$val] = true;
                }
            }
        );
        if(ggood) {
            goods.forEach(
                function(v) {
                    emit(v, {count:1});
                }
            );
        }
    }
    
MAP

    my $reduce = <<RED;
    function(key,values) {
        var result = {count:0.0};
        values.forEach(function(value) {
            result.count += value.count;
                       });
        return result;
    }
RED
    print STDERR $map;
    print STDERR $reduce;
    my $first = $scond ? $json->encode($scond) : '';
    my $second = $otherconds ? $json->encode($otherconds) : '';
    my $checksum = md5_hex(join('_',$first,$second,$list,$val));
    $outputcollname = "$list\_$val\_".$checksum."\_mappings";
    print STDERR "$outputcollname\n";
    $outputcoll = $mongo_db->get_collection($outputcollname);
    if(!$outputcoll->find_one()) {
        my $cmd = Tie::IxHash->new("mapreduce" => "bwa_mapping",
                                   "map" => $map,
                                   "reduce" => $reduce,
                                   "out" => $outputcollname,
                                   "query" => $otherconds
            );
#        if($cond) {
#            $cmd->{cond} = $scond;
#        }
        $mongo_db->run_command($cmd);
    }
}
sub runMapReduce3 {

    my $scond = $cond ? from_json($cond) : undef;
    my $mapconds = [];
    my $otherconds = {};
    
    my $map = <<MAP;
    function() {
        emit(this.$criteria, {count:1});
    }
    
MAP

    my $reduce = <<RED;
    function(key,values) {
        var result = {count:0.0};
        values.forEach(function(value) {
            result.count += value.count;
                       });
        return result;
    }
RED

    my $first = $scond ? $json->encode($scond) : '';
    my $checksum = md5_hex($first);
    $outputcollname = "$criteria\_".$checksum."\_mappings";
    print STDERR "$outputcollname\n";
    $outputcoll = $mongo_db->get_collection($outputcollname);
    if(!$outputcoll->find_one()) {
        print "Loading $outputcollname\n";
        my $cmd = Tie::IxHash->new("mapreduce" => "bwa_mapping",
                                   "map" => $map,
                                   "reduce" => $reduce,
#                                   "JSmode" => 1,
                                   "out" => $outputcollname,
                                   "query" => $scond
            );
#        if($cond) {
#            $cmd->{cond} = $scond;
#        }
        print Dumper $cmd;
        $mongo_db->run_command($cmd);
    }
}

sub dogroup {
    my $red = <<GROUP;
    function(obj,out){
        out.count++;
    }
GROUP
    my $scond = $cond ? from_json($cond) : undef;
    my $cmd = {
        group => {
            'ns' => 'bwa_mapping',
            'key' => {$criteria => 1},
            'initial' => {'count' => 0.0},
            '$reduce' => MongoDB::Code->new(code => $red)
#            'cond' => $condfield { => {'$regex' => qr/^$cond/}}
        }
    };
##    if($condfield eq 'scientific') {
 #       $cmd->{group}->{cond} = {$condfield => {'$regex' => qr/^$cond/}};
 #   }
 #   else {
        $cmd->{group}->{cond} = $scond;
#    }
    $result = $mongo_db->run_command($cmd);
    my @retval;
    my $res = $result->{retval};
    my @srted = sort {$a->{count} <=> $b->{count}} @$res;
    foreach my $ret (@srted) {
        my $id = $ret->{$criteria} ? $ret->{$criteria} : 'Unknown';
        push(@retval, {'_id' => $id,
                       'count' => $ret->{count}});
    }
    $result = {'retval' => \@retval};
}
