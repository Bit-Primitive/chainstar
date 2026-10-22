"""Plug-ins for the tools data teams already run: a dlt source and an Airbyte source.

Both run chainstar's own pipeline into a working warehouse and emit the finished
star-schema tables as a change feed (see feed.py). They need extras:
`chainstar[dlt]` for chainstar.integrations.dlt and `chainstar[airbyte]` for
chainstar.integrations.airbyte; this package and feed.py need nothing extra.
"""
