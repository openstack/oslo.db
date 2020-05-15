# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# -- General configuration ----------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinxcontrib.apidoc',
    'oslo_config.sphinxext',
    'openstackdocstheme',
    'stevedore.sphinxext'
]
# openstackdocstheme options
openstackdocs_repo_name = 'openstack/oslo.db'
openstackdocs_bug_project = 'oslo.db'
openstackdocs_bug_tag = ''

# The master toctree document.
master_doc = 'index'

# General information about the project.
copyright = u'2014, OpenStack Foundation'

# If true, '()' will be appended to :func: etc. cross-reference text.
add_function_parentheses = True

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
add_module_names = True

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# A list of ignored prefixes for module index sorting.
modindex_common_prefix = ['oslo_db.']


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  Major themes that come with
# Sphinx are currently 'default' and 'sphinxdoc'.
html_theme = 'openstackdocs'


# -- sphinxcontrib.apidoc configuration --------------------------------------

apidoc_module_dir = '../../oslo_db'
apidoc_output_dir = 'reference/api'
apidoc_excluded_paths = [
    'tests',
]
