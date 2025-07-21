# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
#
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.


class FixedSizeList(list):
    """
        Customized list to retrieve a fixed sublist / slice of its elements.
        The list will be filled with as many items (default) as needed to
        get a list with the expected size.
    """

    def __init__(self, lst, default=None):
        super(FixedSizeList, self).__init__(lst)
        self._default = default

    def __getitem__(self, key):
        if isinstance(key, slice):
            return [list(self)[i] if 0 <= i < len(self) else self._default for i in range(key.start, key.stop, key.step or 1)]
        return list(self)[key]

    def __getslice__(self, i, j):
        return self.__getitem__(slice(i, j))
