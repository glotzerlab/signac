def get_subject_from_certificate(fn_certificate):
    import subprocess
    try:
        cert_txt = subprocess.check_output(
            ['openssl', 'x509', '-in', fn_certificate,
             '-inform', 'PEM', '-subject', '-nameopt', 'RFC2253']).decode()
    except subprocess.CalledProcessError:
        msg = "Unable to retrieve subject from certificate '{}'."
        raise RuntimeError(msg.format(fn_certificate))
    else:
        lines = cert_txt.split('\n')
        assert lines[0].startswith('subject=')
        return lines[0][len('subject='):].strip()

def fetch(target, timeout = None):
    from threading import Thread, Event
    import queue
    tmp_queue = queue.Queue()
    stop_event = Event()
    def inner_loop():
        from math import tanh
        from itertools import count
        w = (tanh(0.05 * i) for i in count())
        while(not stop_event.is_set()):
            result = target()
            if result is not None:
                tmp_queue.put(result)
                return
        stop_event.wait(max(0.001, next(w)))
    thread_fetch = Thread(target = inner_loop)
    thread_fetch.start()
    thread_fetch.join(timeout = timeout)
    if thread_fetch.is_alive():
        stop_event.set()
        thread_fetch.join()
    try:
        return tmp_queue.get_nowait()
    except queue.Empty:
        raise TimeoutError()

def mongodb_fetch_find_one(collection, spec, timeout = None):
    def target():
        return collection.find_one(spec)
    return fetch(target = target, timeout = timeout)
