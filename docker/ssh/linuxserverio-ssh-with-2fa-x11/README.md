# linuxserverio-ssh-2fa-x11

This is a container based on the [SSH image of LinuxServer.io](https://docs.linuxserver.io/images/docker-openssh-server/).
This image is already used in other tests of Autosubmit, and was tested on EDITO as well.

Here, we use that image as base image, and install and configure two-factor authentication
with Google Authenticator. The image uses a test key for Google Authenticator, and a list
of five backup codes. For testing, we only use the backup codes, as the container is
destroyed after every test (i.e. you can connect up to five times, but normally a
test will require just one connection).

The container also contains the required tools and configuration to support X11 forwarding,
which is used in other tests where Autosubmit configures the Python Paramiko SSH Library
to do X11 forwarding.

The idea of this container is a box to be used in tests to prevent regressions with
users that use platforms with 2FA enabled, and users that rely on X11 for their
workflows.

> NOTE: Use this only in integration tests.

The container with just SSH and Google Authenticator occupies 38.5 MB.
When the X11 libraries are added (`xorg-server`, `xauth`, and `xclock` for testing)
it grows to 395 MB. So be aware of that when running these locally.

> TODO: Maybe we can find a smaller or test X11 server?

## Building

To build the container (note the `--load`, needed when using `buildx`, remove it if not):

```bash
$ docker build --load . -t autosubmit/linuxserverio-ssh-2fa-x11:latest
```

## Running

By default, the container will listen on port 22, it will have X11 installed
and configured, but 2FA will not be enabled.

To run the container, you can use the same arguments as the LinuxServer.io
image, plus the `MFA=<bool>` flag to control whether 2FA is enabled or not:

```bash
$ docker run \
  --rm \
  --name ssh \
  -p1234:22 \
  --env MFA=false \
  --env TZ=Etc/UTC \
  --env SUDO_ACCESS=false \
  --env USER_NAME=as_user \
  --env USER_PASSWORD=password \
  --env PUID=1000 \
  --env PGID=1000 \
  --env UMASK=000 \
  --env PASSWORD_ACCESS=true \
  autosubmit/linuxserverio-ssh-2fa-x11:latest
```

To connect via SSH, you will need to run the following (note: this can be
automated in a Pytest fixture!):

```bash
$ docker exec -ti ssh /bin/bash
root@c55bcd13ae8f:/# mkdir /root/.ssh
root@c55bcd13ae8f:/# echo "ssh-rsa AAAAB3...." > /root/.ssh/authorized_keys
root@c55bcd13ae8f:/# 
exit
$ ssh -X root@localhost -p 1234
```

At this point, you should be connected, and you should be able to run the
`xclock` program in the SSH server to verify that X11 forwarding is working.

To run it with 2FA, you must specify the environment variable `MFA=true`
when running the container:

```bash
$ docker run \
  ... \
  --env MFA=true \
  ... \
  autosubmit/linuxserverio-ssh-2fa-x11:latest
```

Repeat the previous steps for the SSH key, then try to connect via SSH.

When challenged for the SSH 2FA code, you can use the first of the list of
backup codes, `55192054`. Remember that that code is a one-time use.

The current secret for Google Authenticator is `X5C4VLHDCECGFHPP3PHDCS7KAE`,
and the backup codes available are:

- 55192054
- 18998816
- 51868999
- 99315827
- 42932878

If you rebuild the image, just `docker login` and `docker push`, taking
care to use the correct image, and testing it as well.

Happy testing!
