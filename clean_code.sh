#!/bin/bash

files=`git status -s | awk '{print $2}'`

for line in $files; do
	if [[ -d $line ]]; then
		continue
	fi
	case $line in
		*.dtp)
			continue
			;;
		*.sh)
			continue
			;;
		*ucx*)
			continue
			;;
		*libfabric*)
			continue
			;;
		*install.sh)
			continue
			;;
		*.txt*)
			continue
			;;
		*clean_code*)
			continue
			;;
		*Makefile*)
			continue
			;;
	esac

	echo "./maint/code-cleanup.sh $line"
	./maint/code-cleanup.sh $line
	
done
