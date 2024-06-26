---
title: Horreum Clients
description: Reference guide for Horreum clients
date: 2024-04-19
weight: 2
---

Starting from version `0.13`, Horreum comes with some autogenerated clients out of the box that can be used to interact with
the Horreum server programmatically.

Right now there are two clients, one for [`Python`](https://github.com/Hyperfoil/horreum-client-python/) and the other one for [`Go`](https://github.com/Hyperfoil/horreum-client-golang/).

Both clients are autogenerated using [Kiota](https://learn.microsoft.com/en-us/openapi/kiota/) openapi generator, and at the moment of writing the autogenerated client is directly exposed
through the `HorreumClient` instance. Therefore, the interaction with the raw client follows the Kiota experience, more details can be found in the [Kiota documentation](https://learn.microsoft.com/en-us/openapi/kiota/experience).

## Prerequisites

In the below sections you can find some usage examples for both clients, the only precondition is that you must have Horreum server up and running (and accessible).

You can find more details on how to setup Horreum server in the [Get Started](/docs/tutorials/get-started) section.

## Python Client

Install the latest Horreum client

```bash
pip install horreum
```

Import required Python libraries
```python
import asyncio
```

> `asyncio` is required because the client leverages it to perform async requests to the server.

```python
from horreum import new_horreum_client
```

> `new_horreum_client` utility function to setup the `HorreumClient` instance

Initialize the Horreum client

```python
client = await new_horreum_client(base_url="http://localhost:8080", username="user", password="secret")
```

Now let's start playing with it

```python
# sync call
print("Client version:", client.version())

# async call
print("Server version:", (await client.raw_client.api.config.version.get()).version)
```

As you might have noticed, the Kiota api follows the REST url pattern. The previous async call was the equivalent of doing 
`curl -X 'GET' 'http://localhost:8080/api/config/version'`. The URL pattern `api/config/version` is replaced by dot notation
`api.config.version.<HTTP_METHOD>`.

## Golang Client

Install the latest Horreum client

```bash
go get github.com/hyperfoil/horreum-client-golang
```

Import required Go libraries
```go
import (
    "github.com/hyperfoil/horreum-client-golang/pkg/horreum"
)
```

Define username and password

```go
var (
	username = "user"
	password = "secret"
)
```

Initialize the Horreum client

```go
client, err := horreum.NewHorreumClient("http://localhost:8080", &username, &password)
if err != nil {
    log.Fatalf("error creating Horreum client: %s", err.Error())
}
```

Now let's start playing with it

```go
// sync call
fmt.Printf("Server version: %s\n", *v.GetVersion())

// async call
v, err := client.RawClient.Api().Config().Version().Get(context.Background(), nil)
if err != nil {
    log.Fatalf("error getting server version: %s", err.Error())
    os.Exit(1)
}
```

Similarly to the Python example, the previous async call was the equivalent of doing
`curl -X 'GET' 'http://localhost:8080/api/config/version'`. The URL pattern `api/config/version` is replaced by method chain invocation
`api().config().version().<HTTP_METHOD>`. Here you need to provide your own `context` to the actual HTTP method call.
