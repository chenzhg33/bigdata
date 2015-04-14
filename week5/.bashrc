# .bashrc

# User specific aliases and functions

alias rm='rm -i'
alias cp='cp -i'
alias mv='mv -i'

run_mapreduce() {
	hadoop jar /usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming-2.0.0-mr1-cdh4.1.1.jar -mapper "python $1" -reducer "python $2" -file $1 -file $2 -input $3 -output $4
}
run_hadoop_java() {
	if [ $# -gt 2 ]; then
		echo arg1: $1 arg2: $2 arg3: $3
		name=`echo $1 | cut -d '.' -f 1`
		hadoop com.sun.tools.javac.Main $1

		if [ $? -eq 0 ]; then
			echo compile $1.java
		else
			echo compile failed
			return
		fi

		jar cf $name.jar $name*.class 

		if [ $? -eq 0 ]; then
			echo create $name.jar
		else
			echo create jar failed
			return
		fi

		hrm -r $3
		echo running...............
		hadoop jar $name.jar $name $2 $3  &&
		hget $3
	else
		echo need more three argument.
	fi
}

alias hs=run_mapreduce
alias hls='hadoop fs -ls'
alias hput='hadoop fs -put'
alias htail='hadoop fs -tail'
alias hmv='hadoop fs -mv'
alias hrm='hadoop fs -rm'
alias hmkdir='hadoop fs -mkdir'
alias hget='hadoop fs -get'
alias grep='grep --color'

export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar


# Source global definitions
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi
