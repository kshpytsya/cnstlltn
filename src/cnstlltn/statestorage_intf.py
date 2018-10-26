import zope.interface


class IStateStorage(zope.interface.Interface):
    """
    """

    state = zope.interface.Attribute("")

    def open_and_read(read_cb):
        """
        """

    def close():
        """
        """

    def write(write_cb):
        """
        """
