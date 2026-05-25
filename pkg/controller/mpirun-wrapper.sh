#!/bin/bash
# Usage:
# launcher: mpirun-wrapper -c user_cmd -i init_cmd -d end_cmd
# worker: mpirun-wrapper -i init_cmd

if [ "${MPI_LOG}" == "DEBUG" ]; then
	set -x
fi

while getopts ":c:i:d:" arg; do
	case $arg in
	c) user_cmd=$OPTARG ;;
	i) init_cmd=$OPTARG ;;
	d) end_cmd=$OPTARG ;;
	esac
done

cleanup() {
	echo "[MPI] exiting with signal $(date +%s)"
	pkill -P $$
	wait
	exit 1
}

trap cleanup SIGINT SIGTERM

echo "##########################################################################"
echo "############################# MPI JOB START ##############################"
echo "##########################################################################"

workdir=/root/
timeout_time=$(($(date +%s) + ${MPI_TIMEOUT:-600}))

cd $workdir

if [ "${MPI_RUN}" == "0" ]; then
	echo "[MPI] mpirun-wrapper ${MPI_RUN}"
	echo "[MPI] run command $(date +%s) [$user_cmd]"
	/bin/bash -c "$user_cmd"
	exit_code=$?
	echo "[MPI] user exit code: $exit_code"
	exit $exit_code
fi

if [ -f ./consumer.tar ]; then
	tar -vxf ./consumer.tar
elif [ -f ./consumer.zip ]; then
	unzip -o consumer.zip
fi

passwd -d root

if [ ! -z "${PASSWORD}" ]; then
	echo "root:${PASSWORD}" | chpasswd
fi

prerun="/bin/bash -c "

bashrc="/root/.bashrc"

export BASH_ENV=$bashrc

while [ -z "${CHIEF_IP}" ]; do
	source /etc/mpi/environ
	if [[ $(date +%s) -lt $timeout_time ]]; then
		sleep 1
	else
		echo "[MPI] waiting pod ready timeout"
		exit 1
	fi
done

## add env for external login
env_keys=(
	# NCCL / InfiniBand
	NCCL_IB_TC NCCL_IB_DISABLE NCCL_IB_GID_INDEX NCCL_IB_HCA NCCL_IB_TIMEOUT NCCL_IB_SL
	NCCL_SOCKET_IFNAME
)
for key in "${env_keys[@]}"; do
	[[ -n "${!key}" ]] && echo "export ${key}=${!key}" >>$bashrc
done

echo "source /etc/mpi/environ" >>$bashrc

echo "* soft memlock unlimited" >>/etc/security/limits.conf
echo "* hard memlock unlimited" >>/etc/security/limits.conf

## run init command
if [ ! -z "$init_cmd" ]; then
	$prerun "$init_cmd"
fi

if [ -f "/usr/sbin/sshd" ]; then
	max_attempts=20
	attempt=1
	while [ $attempt -le $max_attempts ]; do
		echo "[MPI] Starting sshd"
		/usr/sbin/sshd
		sleep 1

		if pgrep sshd >/dev/null; then
			echo "[MPI] sshd started"
			break
		fi
		if [ $attempt -eq $max_attempts ]; then
			echo "[MPI] Failed to start sshd"
			exit 1
		fi
		sleep 5
		attempt=$((attempt + 1))
	done
else
	echo "[MPI] sshd binary not found"
fi

wait_for_hosts_ready() {
	while read -r line; do
		host=$(echo "$line" | cut -d' ' -f1)
		$prerun "ssh -T -o BatchMode=yes ${host} </dev/null" && continue
		return 1
	done <$OMPI_MCA_orte_default_hostfile
	return 0
}

barrier() {
	if [ "${INDEX}" != "0" ]; then
		return 0
	fi
	if [ "${MPI_RUN}" == "1" ]; then
		return 0
	fi
	if [ ! -f "/usr/sbin/sshd" ]; then
		echo "[MPI] sshd not found"
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
	echo "[MPI] waiting for all peers to be ready..."
	if barrier; then
		echo "[MPI] error log above can be ignored"
		echo "[MPI] run command $(date +%s) [$user_cmd]"
		$prerun "$user_cmd" &
		cmd_pid=$!
		wait $cmd_pid
		exit_code=$?
		if [ ! -z "$end_cmd" ]; then
			$prerun "$end_cmd"
		fi
		echo "[MPI] user exit code: $exit_code"
		exit $exit_code
	else
		echo "[MPI] waiting worker ready timeout"
		exit 1
	fi
else
	echo "[MPI] run idle $(date +%s)"
	while true; do sleep 3; done
fi
