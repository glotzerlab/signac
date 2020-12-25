# The MIT License (MIT)
#
# Copyright (c) 2014, 2015 Adrian Price-Whelan & Dan Foreman-Mackey
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
"""MPIPool for MPI-based multiprocessing-like process pools.

This 3rd party module is copied from https://github.com/adrn/mpipool."""

from deprecation import deprecated

__all__ = ["MPIPool"]
__version__ = "0.0.1"

"""
THIS MODULE IS DEPRECATED!
"""


@deprecated(
    deprecated_in="1.3",
    removed_in="2.0",
    current_version=__version__,
    details="The mpipool module is deprecated.",
)
class MPIPool:
    """
    A pool that distributes tasks over a set of MPI processes using
    mpi4py. MPI is an API for distributed memory parallelism, used
    by large cluster computers. This class provides a similar interface
    to Python's multiprocessing Pool, but currently only supports the
    :func:`map` method.

    Contributed initially by `Joe Zuntz <https://github.com/joezuntz>`_.

    Parameters

    comm : (optional)
        The ``mpi4py`` communicator.

    debug : bool (optional)
        If ``True``, print out a lot of status updates at each step.

    loadbalance : bool (optional)
        if ``True`` and the number of tasks is greater than the
        number of processes, tries to loadbalance by sending out
        one task to each cpu first and then sending out the rest
        as the cpus get done.
    """

    def __init__(self, comm=None, debug=False, loadbalance=False):
        if comm is None:
            # Late import of the MPI constant is necessary, to avoid
            # early mpi initialization, which causes critital errors
            # on badly configured systems.
            from mpi4py import MPI

            self.comm = MPI.COMM_WORLD
        else:
            self.comm = comm
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size() - 1
        self.debug = debug
        self.function = _error_function
        self.loadbalance = loadbalance
        if self.size == 0:
            raise ValueError(
                "Tried to create an MPI pool, but there "
                "was only one MPI process available. "
                "Need at least two."
            )

    def is_master(self):
        """
        Is the current process the master?

        """
        return self.rank == 0

    def wait(self):
        """
        If this isn't the master process, wait for instructions.

        """
        from mpi4py import MPI

        if self.is_master():
            raise RuntimeError("Master node told to await jobs.")

        status = MPI.Status()

        while True:
            # Event loop.
            # Sit here and await instructions.
            if self.debug:
                print(f"Worker {self.rank} waiting for task.")

            # Blocking receive to wait for instructions.
            task = self.comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
            if self.debug:
                print(f"Worker {self.rank} got task {task} with tag {status.tag}.")

            # Check if message is special sentinel signaling end.
            # If so, stop.
            if isinstance(task, _close_pool_message):
                if self.debug:
                    print(f"Worker {self.rank} told to quit.")
                break

            # Check if message is special type containing new function
            # to be applied
            if isinstance(task, _function_wrapper):
                self.function = task.function
                if self.debug:
                    print(
                        f"Worker {self.rank} replaced its task function: {self.function}."
                    )
                continue

            # If not a special message, just run the known function on
            # the input and return it asynchronously.
            result = self.function(task)
            if self.debug:
                print(
                    f"Worker {self.rank} sending answer {result} with tag {status.tag}."
                )
            self.comm.isend(result, dest=0, tag=status.tag)

    def map(self, function, tasks, ntask=None, callback=None):
        """
        Like the built-in :func:`map` function, apply a function to all
        of the values in a list and return the list of results.

        Parameters

        function : callable
            The function to apply to each element in the list.

        tasks :
            A list of tasks -- each element is passed to the input
            function.

        callback : callable (optional)
            A callback function to call on each result.

        """
        from mpi4py import MPI

        if ntask is None:
            ntask = len(tasks)

        # If not the master just wait for instructions.
        if not self.is_master():
            self.wait()
            return

        if function is not self.function:
            if self.debug:
                print(f"Master replacing pool function with {function}.")

            self.function = function
            F = _function_wrapper(function)

            # Tell all the workers what function to use.
            requests = []
            for i in range(self.size):
                r = self.comm.isend(F, dest=i + 1)
                requests.append(r)

            # Wait until all of the workers have responded. See:
            #       https://gist.github.com/4176241
            MPI.Request.waitall(requests)

        if (not self.loadbalance) or (ntask <= self.size):
            # Do not perform load-balancing - the default load-balancing
            # scheme emcee uses.

            # Send all the tasks off and wait for them to be received.
            # Again, see the bug in the above gist.
            requests = []
            for i, task in enumerate(tasks):
                worker = i % self.size + 1
                if self.debug:
                    print(f"Sent task {task} to worker {worker} with tag {i}.")
                r = self.comm.isend(task, dest=worker, tag=i)
                requests.append(r)

            MPI.Request.waitall(requests)

            # Now wait for the answers.
            results = []
            for i in range(ntask):
                worker = i % self.size + 1
                if self.debug:
                    print(f"Master waiting for worker {worker} with tag {i}")
                result = self.comm.recv(source=worker, tag=i)

                if callback is not None:
                    callback(result)

                results.append(result)

            return results

        else:
            # Perform load-balancing. The order of the results are likely to
            # be different from the previous case.
            for i, task in enumerate(tasks[0 : self.size]):
                worker = i + 1
                if self.debug:
                    print(f"Sent task {task} to worker {worker} with tag {i}.")
                # Send out the tasks asynchronously.
                self.comm.isend(task, dest=worker, tag=i)

            ntasks_dispatched = self.size
            results = [None] * ntask
            for itask in range(ntask):
                status = MPI.Status()
                # Receive input from workers.
                result = self.comm.recv(
                    source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status
                )
                worker = status.source
                i = status.tag

                if callback is not None:
                    callback(result)

                results[i] = result
                if self.debug:
                    print(f"Master received from worker {worker} with tag {i}")

                # Now send the next task to this idle worker (if there are any
                # left).
                if ntasks_dispatched < ntask:
                    task = tasks[ntasks_dispatched]
                    i = ntasks_dispatched
                    if self.debug:
                        print(f"Sent task {task} to worker {worker} with tag {i}.")
                    # Send out the tasks asynchronously.
                    self.comm.isend(task, dest=worker, tag=i)
                    ntasks_dispatched += 1

            return results

    def bcast(self, *args, **kwargs):
        """
        Equivalent to mpi4py :func:`bcast` collective operation.
        """
        return self.comm.bcast(*args, **kwargs)

    def close(self):
        """
        Just send a message off to all the pool members which contains
        the special :class:`_close_pool_message` sentinel.

        """
        if self.is_master():
            for i in range(self.size):
                self.comm.isend(_close_pool_message(), dest=i + 1)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class _close_pool_message:
    def __repr__(self):
        return "<Close pool message>"


class _function_wrapper:
    def __init__(self, function):
        self.function = function


def _error_function(task):
    raise RuntimeError("Pool was sent tasks before being told what function to apply.")
