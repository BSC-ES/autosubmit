Files to build an EasyBuild installation for Autosubmit.

Refs:

- https://earth.bsc.es/gitlab/es/autosubmit/-/issues/1257
- https://earth.bsc.es/gitlab/es/autosubmit/-/merge_requests/499
- https://earth.bsc.es/gitlab/es/autosubmit/-/merge_requests/474

## Testing with Docker (WIP)

> This documentation was used during the tests to develop the new recipes.
> Use it with care, in case you need to test this without access to the
> physical servers.

Building locally, to test it first:

```bash
$ docker login ghcr.io -u $USER
$ docker run --rm -ti \
    -u easybuild \
    -v $(pwd -P)/recipes/autosubmit-4.X-dev-foss-2021b-Python-3.9.6.eb:/home/easybuild/autosubmit-4.X-dev-foss-2021b-Python-3.9.6.eb \
    -w /home/easybuild ghcr.io/easybuilders/ubuntu-20.04:2024-01-17-7559408166.106 \
    /bin/bash
easybuild@e9c0fc4f3b5d:~$ pip install easybuild
easybuild@e9c0fc4f3b5d:~$ export PATH="$PATH:/home/easybuild/.local/bin:/usr/lmod/8.6.12/libexec/"
easybuild@e9c0fc4f3b5d:~$ EB_VERBOSE=1 eb autosubmit-*.eb --rebuild
```
