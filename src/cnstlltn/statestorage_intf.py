import zope.interface


class IStateStorage(zope.interface.Interface):
    """
    """

    state = zope.interface.Attribute("")

    def open(*, timeout):
        """
        """

    def close():
        """
        """

    def set(key, value):
        """
        """
