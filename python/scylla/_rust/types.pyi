class UnsetType:
    """
    Type of the `Unset` singleton.

    `Unset` means no value provided and is distinct from
    `None`, which means explicitly set to no value.

    Used for options like `serial_consistency` and `request_timeout`,
    where the two cases may result in different behaviour.
    """

    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...
