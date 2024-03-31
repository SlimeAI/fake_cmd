# fake_cmd

fake_cmd provides a simulated remote command line based on file communication, in order to send commands from the client to the server for execution. It applies to situations where two computers cannot be connected through the network (which means ``ssh`` is impossible), but can be mounted onto the same hard disk.

## Get Started

### Requirements

``slime_core`` is a necessary dependency, and install it as follows:

```bash
pip install slime_core
```

``pexpect`` is a recommended dependency (if you run the server on a Unix platform):

```bash
pip install pexpect
```

### Start the Server and the Client

On the server computer which you want to run your code on, start the server as follows:

```Python
from fake_cmd.core.server import Server

if __name__ == '__main__':
    Server('/specify/a/hard/disk/path/here').run()
```

On the client, start the client as follows:

```Python
from fake_cmd.core.client import CLI

if __name__ == '__main__':
    CLI('/same/path/as/the/server').run()
```
