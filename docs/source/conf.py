# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('../..'))


# -- Project information -----------------------------------------------------

project = 'grizly'
copyright = '2019, Alessio'
author = 'Alessio'

# The full version, including alpha/beta/rc tags
release = '0.2'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ['sphinx.ext.autodoc'
                , 'sphinx.ext.napoleon'
                , 'sphinx.ext.autosummary'
                , 'nbsphinx'
                , 'jupyter_sphinx.execute'
                ]

# nbsphinx_execute = 'never'
nbsphinx_allow_errors = True
nbsphinx_requirejs_path = ''

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#
# source_suffix = ['.rst', '.md']
source_suffix = ['.rst', '.md']


# The master toctree document.
master_doc = 'index'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['**.ipynb_checkpoints']


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ['_static']
html_show_sourcelink = True

master_doc = 'index'

# -----------------------------------------------------------------------------
# Autosummary
# -----------------------------------------------------------------------------

autosummary_generate = True
autodoc_default_flags = ['members', 'inherited-members']

# spell checking
spelling_lang = 'en_US'
spelling_word_list_filename = 'spelling_wordlist.txt'
spelling_show_suggestions = True


# try to exclude deprecated
def skip_deprecated(app, what, name, obj, skip, options):
    if hasattr(obj, "func_dict") and "__deprecated__" in obj.func_dict:
        print("skipping " + name)
        return True
    return skip or False

def setup(app):
    app.connect('autodoc-skip-member', skip_deprecated)
    try:
        from sphinx.ext.autosummary import Autosummary
        from sphinx.ext.autosummary import get_documenter
        from docutils.parsers.rst import directives
        from sphinx.util.inspect import safe_getattr
        import re

        class AutoAutoSummary(Autosummary):

            option_spec = {
                'methods': directives.unchanged
            }

            required_arguments = 1

            @staticmethod
            def get_members(obj, typ, include_public=None):
                if not include_public:
                    include_public = []
                items = []
                for name in dir(obj):
                    try:
                        documenter = get_documenter(app, safe_getattr(obj, name), obj)
                    except AttributeError:
                        continue
                    if documenter.objtype == typ:
                        items.append(name)
                public = [x for x in items if x in include_public or not x.startswith('_')]
                return public, items

            def run(self):
                clazz = self.arguments[0]
                try:
                    (module_name, class_name) = clazz.rsplit('.', 1)
                    m = __import__(module_name, globals(), locals(), [class_name])
                    c = getattr(m, class_name)
                    if 'methods' in self.options:
                        _, methods = self.get_members(c, 'method', ['__init__'])

                        self.content = ["~%s.%s" % (clazz, method) for method in methods if not method.startswith('_')]
                finally:
                    return super(AutoAutoSummary, self).run()

        app.add_directive('autoautosummary', AutoAutoSummary)
    except BaseException as e:
        raise e
