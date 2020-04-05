import ipywidgets as w
from ..tools.sqldb import SQLDB
from ..store import Store


def get_subqueries(store_path):
    store = Store(store_path)
    return list(store.data.keys())


class SubqueryUI:
    def __init__(self, store_path):
        self.qf = None
        self.subquery = ""
        self.database = ""
        self.store_path = store_path
        self.label = w.Label("Build Your Subquery")
        self.schema = w.Text(
            value="base_views",
            description="Schema:",
            disabled=False,
            layout=w.Layout(height="auto", width="auto"),
        )
        self.table = w.Text(
            value="sales_monthly_close",
            description="Table:",
            disabled=False,
            layout=w.Layout(height="auto", width="auto"),
        )
        self.engine = w.Text(
            value="mssql+pyodbc://acoe_redshift",
            description="Engine:",
            disabled=False,
            layout=w.Layout(height="auto", width="auto"),
        )
        self.button = w.Button(
            description="Get Columns",
            disabled=False,
            layout=w.Layout(height="auto", width="auto"),
        )
        self.btn_update_store = w.Button(
            description="Update Store",
            disabled=False,
            layout=w.Layout(height="auto", width="auto"),
        )
        self._as = w.Text(
            value="",
            description="As:",
            disabled=False,
            layout=w.Layout(height="auto", width="auto"),
        )
        self.type = w.Dropdown(
            options=["dim", "num", ""], description="Type:", value="dim", disabled=False
        )
        self.group_by = w.Dropdown(
            options=["group", "sum"],
            description="Group By:",
            value="group",
            disabled=False,
        )
        self.order_by = w.Text(
            value="",
            description="Oder By:",
            disabled=False,
            layout=w.Layout(height="auto", width="auto"),
        )
        self.custom_type = w.Text(
            value="",
            description="Custom Type:",
            disabled=False,
            layout=w.Layout(height="auto", width="auto"),
        )
        self.options = None
        self.output = None

    def _btn_build_subquery_step_2(self, button):
        self.label.value = "Select Your Columns"
        sqldb = SQLDB(db=self.database)
        cols = sqldb.get_columns(schema=self.schema.value, table=self.table.value)
        self.options = [w.Checkbox(description=col) for col in cols]
        self._options = w.Box(
            self.options, layout=w.Layout(height="200px", width="auto", display="grid")
        )
        self.btn_update_store.on_click(self._btn_update_store)
        self.output.center = self._options

        right_sidebar = [
            self.btn_update_store,
            self._as,
            self.type,
            self.group_by,
            self.order_by,
            self.custom_type,
            self.label,
        ]  # remove self.label
        self.output.right_sidebar = w.VBox(right_sidebar)

    def _btn_update_store(self, button):
        data = self.qf.data
        if self.custom_type != None:
            self.type.value = ""
        field_attrs = {
            "type": self.type.value,
            "as": self._as.value,
            "group_by": self.group_by.value,
            "order_by": self.order_by.value,
            "custom_type": self.custom_type.value,
        }
        for option in self.options:
            self.label.value = str(option.description)
            if option.value == True:
                field_name = option.description
                self.label.value = str(field_name)
                data["select"]["fields"][field_name] = field_attrs
                self.label.value = str(field_name)
        self.label.value = str(self.qf.data)
        try:
            self.qf.remove(["sample_field"])
        except:
            pass
        data["select"]["table"] = self.table.value
        data["select"]["schema"] = self.schema.value
        self.qf.save_json(self.store_path, subquery=self.subquery)

    def build_subquery(self, qf, subquery, database):
        self.database = database
        self.subquery = subquery
        try:
            self.qf = qf.read_json(self.store_path, subquery=subquery)
            self.schema.value = self.qf.data["select"]["schema"]
            self.table.value = self.qf.data["select"]["table"]
            self.engine.value = self.qf.data["select"]["engine"]
        except KeyError:
            data = {
                "select": {
                    "fields": {"sample_field": {"type": "dim"}},
                    "schema": "sample_schema",
                    "table": "sample_table",
                }
            }
            self.qf = qf.read_dict(data=data)
        self.output = w.AppLayout(
            left_sidebar=w.VBox([self.schema, self.table, self.engine, self.button]),
            center=None,
            right_sidebar=None,
            pane_widths=[2, 3, 3],
            pane_heights=[1, 5, 1],
        )
        self.button.on_click(self._btn_build_subquery_step_2)
        return self.output


class FieldUI:
    def __init__(self, store_path):
        self.qf = None
        self.store_path = store_path
        self.label = w.Label("Note you need to reload cell for each new field")
        self.field_name = w.Text(
            value="field_name", description="Field Name:", disabled=False
        )
        self.expression = w.Textarea(
            value="SQL Expression Here",
            description="Expression:",
            disabled=False,
            layout={"width": "100%"},
        )
        self.subqueries = w.Dropdown(options=[])
        self.subquery = w.Text(
            value="subquery_name", description="Subquery Name:", disabled=False
        )
        self.type = w.Text(value="dim", description="Type:", disabled=False)
        self._as = w.Text(value="", description="As:", disabled=False)
        self.group_by = w.Text(value="group", description="Group By:", disabled=False)
        self.fields = w.Dropdown(options=[])
        self.btn_add_to_store = w.Button(description="Add To Store", disabled=False)
        self.btn_update_store = w.Button(description="Update Store", disabled=False)
        self.btn_remove_field = w.Button(description="Remove Field", disabled=False)
        self.btn_add_field = w.Button(description="Add Field", disabled=False)
        self.btn_edit_field = w.Button(description="Edit Field", disabled=False)
        self.output = None

    def _btn_add_field(self, button):
        self.label.value = "You are now adding a new field"
        self.qf = self.qf.read_json(self.store_path, subquery=self.subqueries.value)
        new_out = self.add_field()
        self.output.children += (new_out,)

    def _btn_edit_field(self, button):
        self.label.value = "You are now editing an existing field"
        self.qf = self.qf.read_json(self.store_path, subquery=self.subqueries.value)
        new_out = self.edit_field()
        self.output.children += (new_out,)

    def _btn_update_store(self, button):
        try:
            data = self.qf.data["select"]["fields"][self.fields.value]
            data["expression"] = self.expression.value
            data["as"] = self._as.value
            data["type"] = self.type.value
            data["group_by"] = self.group_by.value
            self.qf.save_json(self.store_path, subquery=self.subqueries.value)
            # self.label.value = "Field {} updated".format(self.fields.value)
            self.label.value = data["as"]
        except:
            self.label.value = "Error. Are you sure this fields exists? "

    def _btn_add_to_store(self, button):
        self.qf.assign(**{self.field_name.value: self.expression.value})
        self.qf.save_json(self.store_path, subquery=self.subqueries.value)

    def _btn_remove_field(self, button):
        self.label.value = "Removed {}".format(self.fields.value)
        self.qf.remove(self.fields.value)
        self.qf.save_json(self.store_path, subquery=self.subqueries.value)

    def build_field(self, store_path, qf):
        self.qf = qf
        self.store_path = store_path
        self.subqueries.options = get_subqueries(store_path)
        row_1 = w.HBox([self.label])
        row_2 = w.HBox([self.subqueries])
        row_3 = w.HBox([self.btn_add_field, self.btn_edit_field])
        first_out = w.VBox([row_1, row_2, row_3])
        self.output = first_out
        self.btn_add_field.on_click(self._btn_add_field)
        self.btn_edit_field.on_click(self._btn_edit_field)
        self.label.value = str(qf.data)
        return self.output

    def add_field(self):
        row_1 = w.VBox([self.field_name, self.type, self._as, self.group_by])
        row_2 = w.HBox([self.expression])
        row_3 = w.HBox([self.btn_add_to_store])
        self.btn_add_to_store.on_click(self._btn_add_to_store)
        return w.VBox([row_1, row_2, row_3])

    def edit_ui(self):
        field = self.qf.data["select"]["fields"][self.fields.value]
        self._as.value = field["as"]
        self.type.value = field["type"]
        self.group_by.value = field["group_by"]
        self.expression.value = field["expression"]

    def edit_field(self):
        fields = self.qf.get_fields()
        self.fields.options = fields
        self.fields.on_trait_change(self.edit_ui, name="value")
        row_1 = w.VBox([self.fields, self.type, self._as, self.group_by])
        row_2 = w.HBox([self.expression])
        row_3 = w.HBox([self.btn_update_store, self.btn_remove_field])
        self.edit_ui()
        self.btn_update_store.on_click(self._btn_update_store)
        self.btn_remove_field.on_click(self._btn_remove_field)
        return w.VBox([row_1, row_2, row_3])

