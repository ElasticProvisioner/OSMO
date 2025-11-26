<!-- SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0 -->

<a id="cli-reference-config-update"></a>

# osmo config update

Update a configuration

```default
osmo config update [-h] config_type [name] [--file FILE] [--description DESCRIPTION] [--tags TAGS [TAGS ...]]

Available config types (CONFIG_TYPE): BACKEND, BACKEND_TEST, DATASET, POD_TEMPLATE, POOL, RESOURCE_VALIDATION, ROLE, SERVICE, WORKFLOW

Ex. osmo config update SERVICE
Ex. osmo config update POOL my-pool --description "Updated pool settings" --tags production high-priority
Ex. osmo config update BACKEND my-backend --file config.json
```

## Positional Arguments

`config_type`
: Config type to update (CONFIG_TYPE)

`name`
: Optional name of the config to update

## Named Arguments

* **--file-f**: 

Path to a JSON file containing the updated config
* **--description-d**: 

Description of the config update
* **--tags-t**: 

Tags for the config update

## Examples

Update a service configuration:

```bash
$ osmo config show SERVICE cli_config latest_version
Key              Value
=========================
latest_version   6.0.0

$ osmo config update SERVICE
Successfully updated SERVICE config

$ osmo config show SERVICE cli_config latest_version
Key              Value
=========================
latest_version   6.0.1
```

Update a backend configuration from a file:

```bash
$ osmo config update BACKEND my-backend --file config.json
Successfully updated BACKEND config
```

Update with description and tags:

```bash
$ osmo config update POOL my-pool --description "Updated pool settings" --tags production high-priority
Successfully updated POOL config
```
