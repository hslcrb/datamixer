import pandas as pd
import numpy as np
from PySide6.QtCore import Qt, QAbstractTableModel

class PandasModel(QAbstractTableModel):
    """
    High-performance Enterprise Model for Datamixer.
    Optimized for massive datasets with intelligent alignment and interactive editing.
    """
    def __init__(self, data):
        super().__init__()
        self._data = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
        self._columns = self._data.columns.tolist()
        self.read_only = True # Default to read-only for safety

    def rowCount(self, parent=None):
        return self._data.shape[0]

    def columnCount(self, parent=None):
        return self._data.shape[1]

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
            
        row, col = index.row(), index.column()
        val = self._data.iat[row, col]

        if role in (Qt.DisplayRole, Qt.EditRole):
            if pd.isna(val) or val is None:
                return "" if role == Qt.EditRole else "—"
            return str(val)
        
        elif role == Qt.TextAlignmentRole:
            if isinstance(val, (int, float, np.number)):
                return Qt.AlignRight | Qt.AlignVCenter
            return Qt.AlignLeft | Qt.AlignVCenter
            
        elif role == Qt.ForegroundRole:
            if pd.isna(val) or val is None:
                from PySide6.QtGui import QColor
                return QColor("#565f89")
            
        return None

    def setData(self, index, value, role=Qt.EditRole):
        if role == Qt.EditRole and not self.read_only:
            try:
                row, col = index.row(), index.column()
                # Attempt to preserve original data type
                orig_val = self._data.iat[row, col]
                if isinstance(orig_val, (int, np.integer)):
                    value = int(value)
                elif isinstance(orig_val, (float, np.float64)):
                    value = float(value)
                
                self._data.iat[row, col] = value
                self.dataChanged.emit(index, index, [Qt.DisplayRole])
                return True
            except:
                return False
        return False

    def flags(self, index):
        base_flags = super().flags(index)
        if not self.read_only:
            return base_flags | Qt.ItemIsEditable
        return base_flags

    def headerData(self, col, orientation, role):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return str(self._columns[col])
        return None
