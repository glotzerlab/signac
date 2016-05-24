N=$1
T=$2
p_n=$3
p_d=${4-1}

V=$(expr $N \* $T \* $p_n / $p_d)
echo $V
