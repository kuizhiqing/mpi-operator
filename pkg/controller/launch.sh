#!/bin/bash
# Usage:
# launcher: launch -c user_cmd -i init_cmd -d end_cmd
# worker: launch -i init_cmd

if [ "${LAUNCH_LOG}" == "DEBUG" ]; then
	set -x
fi

while getopts ":c:i:d:" arg; do
	case $arg in
	c) user_cmd=$OPTARG ;;
	i) init_cmd=$OPTARG ;;
	d) end_cmd=$OPTARG ;;
	esac
done

trap "exit 1" 15

timeout_time=$(($(date +%s) + ${LAUNCH_TIMEOUT:-600}))

cd $workdir

if [ "${LAUNCH_RUN}" == "0" ]; then
	echo "[LAUNCH] launch ${LAUNCH_RUN}"
	echo "[LAUNCH] run command $(date +%s) [$user_cmd]"
	/bin/bash -c "$user_cmd"
	exit_code=$?
	echo "[LAUNCH] user exit code: $exit_code"
	exit $exit_code
fi

bashc="/bin/bash -c "
bashrc=~/.bashrc

export BASH_ENV=~/.bashrc

while [ -z "${CHIEF_IP}" ]; do
	source /etc/launch/environ
	if [[ $(date +%s) -lt $timeout_time ]]; then
		sleep 1
	else
		echo "[LAUNCH] waiting pod ready timeout"
		exit 1
	fi
done

## add env for external login
echo "export LOCAL_IP=$LOCAL_IP" >>$bashrc
echo "export HOST_GPU_NUM=$HOST_GPU_NUM" >>$bashrc
echo "export HOST_NUM=$HOST_NUM" >>$bashrc
echo "export NODE_NUM=$NODE_NUM" >>$bashrc
echo "export INDEX=$INDEX" >>$bashrc

echo "source /etc/launch/environ" >>$bashrc

## run init command
if [ ! -z "$init_cmd" ]; then
	$bashc "$init_cmd"
fi

/usr/sbin/sshd

wait_for_hosts_ready() {
	while read -r line; do
		host=$(echo "$line" | cut -d' ' -f1)
		$bashc "ssh -T -o BatchMode=yes ${host} </dev/null" && continue
		return 1
	done <$OMPI_MCA_orte_default_hostfile
	return 0
}

barrier() {
	if [ ! -f "/usr/sbin/sshd" ]; then
		return 0
	fi
	while [[ $(date +%s) -lt $timeout_time ]]; do
		if wait_for_hosts_ready; then
			return 0
		else
			sleep 2
		fi
	done
	return 1
}

## run user command
if [ ! -z "$user_cmd" ] && [ -f "$OMPI_MCA_orte_default_hostfile" ]; then
	echo "[LAUNCH] waiting for all peers to be ready..."
	if barrier; then
		echo "[LAUNCH] error log above can be ignored"
		echo "[LAUNCH] run command $(date +%s) [$user_cmd]"
		$bashc "$user_cmd"
		exit_code=$?
		if [ ! -z "$end_cmd" ]; then
			$bashc "$end_cmd"
			exit_code=$?
		fi
		echo "[LAUNCH] user exit code: $exit_code"
		exit $exit_code
	else
		echo "[LAUNCH] waiting worker ready timeout"
		exit 1
	fi
else
	echo "[LAUNCH] run idle $(date +%s)"
	while true; do sleep 3; done
fi
