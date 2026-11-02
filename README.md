# EchoLink provider for RPT Advanced

This package connects RPT Advanced nodes to EchoLink stations. It supports
account registration, station lists, direct IPv4 UDP, a user-provided EchoLink
Proxy, GSM voice, optional narrowband Speex, and incoming human text.

Install `librptadvecholink1` for the runtime library. Its required GSM, Speex,
and jitter providers are installed with it. Install `librptadvecholink-dev`
only when building software against its public C header.

Configure accounts in your controller's `rpt_advanced.conf`, not in this
library. Each local node needs a separate validated EchoLink account and a
private password file owned by its service user with mode `0600`.
See the controller's [EchoLink operating guide](https://github.com/cpeter1207/rpt_advanced/blob/main/doc/echolink.md)
for all settings, port rules, commands, and checks.

A standard EchoLink Proxy uses TCP, normally port 8100. It carries directory,
voice, control, and text traffic. The user supplies the Proxy. HTTP proxies
are not supported. Several accounts need distinct effective public endpoints;
changing local UDP ports alone does not provide them.

Test incoming and outgoing calls, both audio directions, silence, text,
disconnect, and reload with the stations and Proxy you will use before placing
a node in service. A successful directory login does not prove that outside
stations can reach the node. Check the account log after startup or reload.

For library build instructions and required checks, see [QUALITY.md](QUALITY.md).
