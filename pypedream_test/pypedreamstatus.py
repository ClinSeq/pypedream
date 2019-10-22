class PypedreamStatus:
    def __init__(self):
        pass

    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    CANCELLED = "CANCELLED"
    NOT_FOUND = "NOT_FOUND"

    @staticmethod
    def from_slurm(status_string):
        """
        Convert from slurm status to a Pypedream status. From http://slurm.schedmd.com/squeue.html:

        JOB STATE CODES

        Jobs typically pass through several states in the course of their execution. The typical states are PENDING, RUNNING, SUSPENDED, COMPLETING, and COMPLETED. An explanation of each state follows.
        BF BOOT_FAIL
        Job terminated due to launch failure, typically due to a hardware failure (e.g. unable to boot the node or block and the job can not be requeued).
        CA CANCELLED
        Job was explicitly cancelled by the user or system administrator. The job may or may not have been initiated.
        CD COMPLETED
        Job has terminated all processes on all nodes with an exit code of zero.
        CF CONFIGURING
        Job has been allocated resources, but are waiting for them to become ready for use (e.g. booting).
        CG COMPLETING
        Job is in the process of completing. Some processes on some nodes may still be active.
        F FAILED
        Job terminated with non-zero exit code or other failure condition.
        NF NODE_FAIL
        Job terminated due to failure of one or more allocated nodes.
        PD PENDING
        Job is awaiting resource allocation.
        PR PREEMPTED
        Job terminated due to preemption.
        R RUNNING
        Job currently has an allocation.
        SE SPECIAL_EXIT
        The job was requeued in a special state. This state can be set by users, typically in EpilogSlurmctld, if the job has terminated with a particular exit value.
        ST STOPPED
        Job has an allocation, but execution has been stopped with SIGSTOP signal. CPUS have been retained by this job.
        S SUSPENDED
        Job has an allocation, but execution has been suspended and CPUs have been released for other jobs.
        TO TIMEOUT
        Job terminated upon reaching its time limit.
         """
        slurm2pypedream = {"PENDING": PypedreamStatus.PENDING,
                           "RUNNING": PypedreamStatus.RUNNING,
                           "SUSPENDED": PypedreamStatus.FAILED,
                           "STOPPED": PypedreamStatus.CANCELLED,
                           "COMPLETING": PypedreamStatus.RUNNING,
                           "COMPLETED": PypedreamStatus.COMPLETED,
                           "CONFIGURING": PypedreamStatus.RUNNING,
                           "CANCELLED": PypedreamStatus.CANCELLED,
                           "FAILED": PypedreamStatus.FAILED,
                           "TIMEOUT": PypedreamStatus.FAILED,
                           "PREEMPTED": PypedreamStatus.FAILED,
                           "BOOT_FAIL": PypedreamStatus.FAILED,
                           "NODE_FAIL": PypedreamStatus.FAILED,
                           "SPECIAL_EXIT": PypedreamStatus.PENDING
                           }

        if status_string in slurm2pypedream:
            return slurm2pypedream[status_string]
        else:
            raise ValueError("Supplied slurm status {} is not a correct job state code.".format(status_string))
