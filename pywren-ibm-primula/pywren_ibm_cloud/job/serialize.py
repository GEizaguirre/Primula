#
# Copyright 2018 PyWren Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import inspect
import os
import logging
from pathlib import Path
from io import BytesIO as StringIO
from pywren_ibm_cloud.libs import glob2
from pywren_ibm_cloud.utils import bytes_to_b64str
from pywren_ibm_cloud.libs.cloudpickle import CloudPickler
from pywren_ibm_cloud.libs.multyvac.module_dependency import ModuleDependencyAnalyzer

logger = logging.getLogger(__name__)


class SerializeIndependent:

    def __init__(self, preinstalls):
        self.preinstalled_modules = preinstalls
        #self.preinstalled_modules.append(['pywren_ibm_cloud', True])
        self._modulemgr = None

    def __call__(self, list_of_objs, include_modules, exclude_modules):
        """
        Serialize f, args, kwargs independently
        """

        #logger.debug("List of object {}".format(list_of_objs))

        self._modulemgr = ModuleDependencyAnalyzer()
        preinstalled_modules = [name for name, _ in self.preinstalled_modules]
        #preinstalled_modules.remove('pyimzml')
        #logger.debug("Preinstalled modules {}".format(preinstalled_modules))
        #logger.debug("Include modules {}".format(include_modules))
        #logger.debug("Exclude modules {}".format(exclude_modules))

        self._modulemgr.ignore(preinstalled_modules)
        if not include_modules:
            self._modulemgr.ignore(exclude_modules)

        #logger.debug("DEV:::SerializeIndependent - Serializing function {}".format(inspect.getsource(list_of_objs[0])))

        cps = []
        strs = []
        for obj in list_of_objs:
            file = StringIO()
            try:
                cp = CloudPickler(file)
                cp.dump(obj)
                #logger.debug("cp {}".format(cp))
                #logger.debug("cp modules {}".format(cp.modules))
                cps.append(cp)
                strs.append(file.getvalue())
            finally:
                file.close()

        # Add modulescp
        direct_modules = set()
        for cp in cps:
            for module in cp.modules:
                try:
                    direct_modules.add(module.__file__)
                except Exception:
                    pass
                self._modulemgr.add(module.__name__)

        #logger.debug("Referenced modules: {}".format(None if not direct_modules else direct_modules))

        mod_paths = set()
        if include_modules is not None and len(include_modules) > 0:
            tent_mod_paths = self._modulemgr.get_and_clear_paths()
            if include_modules:


                for im in include_modules:
                    #logger.debug("Filtering module: {}".format(im))
                    for mp in tent_mod_paths:
                        if im in mp:
                            mod_paths.add(mp)
                            break
            else:
                mod_paths = tent_mod_paths

        return (strs, mod_paths)


def create_module_data(mod_paths):

    module_data = {}
    # load mod paths
    for m in mod_paths:
        if os.path.isdir(m):
            files = glob2.glob(os.path.join(m, "**/*.py"))
            pkg_root = os.path.abspath(os.path.dirname(m))
        else:
            pkg_root = os.path.abspath(os.path.dirname(m))
            files = [m]
        for f in files:
            f = os.path.abspath(f)
            with open(f, 'rb') as file:
                mod_str = file.read()
            dest_filename = Path(f[len(pkg_root)+1:]).as_posix()
            module_data[dest_filename] = bytes_to_b64str(mod_str)

    return module_data
