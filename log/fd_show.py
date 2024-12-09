import os
import stat

_fd_types = (
    ('REG', stat.S_ISREG),
    ('FIFO', stat.S_ISFIFO),
    ('DIR', stat.S_ISDIR),
    ('CHR', stat.S_ISCHR),
    ('BLK', stat.S_ISBLK),
    ('LNK', stat.S_ISLNK),
    ('SOCK', stat.S_ISSOCK)
)

def fd_table_status():
    result = []
    for fd in range(1024):
        try:
            s = os.fstat(fd)
        except Exception:
            continue
        for fd_type, func in _fd_types:
            if func(s.st_mode):
                break
        else:
            fd_type = str(s.st_mode)
        result.append((fd, fd_type))
    return result

def fd_table_status_logify(fd_table_result):
    return ('Open file handles: ' +
            ', '.join(['{0}: {1}'.format(*i) for i in fd_table_result]))

def fd_table_status_str():
    return fd_table_status_logify(fd_table_status())

if __name__=='__main__':
    print(fd_table_status_str())