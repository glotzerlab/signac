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
