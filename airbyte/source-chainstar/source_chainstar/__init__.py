"""The chainstar Airbyte source, as Airbyte's tooling expects to import it.

The connector lives in chainstar itself (chainstar.integrations.airbyte, installed
with `pip install "chainstar[airbyte]"`); this package names it the way
`airbyte-cdk connector test` looks for it: module source_chainstar, class
SourceChainstar.
"""

from chainstar.integrations.airbyte import SourceChainstar, run

__all__ = ["SourceChainstar", "run"]
