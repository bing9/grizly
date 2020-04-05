import ipywidgets as w
from ..ui.qframe import SubqueryUI


class UI():
    
    def __init__(self, store_path):
        self.subquery = ""
        self.database = ""
        self.store_path = store_path
        self.label = w.Label('Build Your Subquery')

        self.btn_subquery = w.Button(description="Create Query")
        self.btn_subquery.on_click(self._btn_create_query)

        self.output = w.AppLayout(
                        left_sidebar=w.VBox([self.btn_subquery])
                        , center=None
                        , right_sidebar=None
                        , pane_widths= [2, 3, 3]
                        , pane_heights=[1, 5, 1]
                         )

    def _btn_create_query(self, button):
        self.output = SubqueryUI(self.store_path).build_subquery(subquery="new_11"
                                                           , database="denodo")
        