#!/usr/bin/env python
# coding: utf-8

import sys, json, os, shutil, base64, zipfile, io, tempfile, re
from typing import List, Optional, Tuple

ENC = "ISO-8859-1"
SMOKE_TEMPLATE_ZIP_PATH = os.path.join(os.path.dirname(__file__), "assets", "SMOKE_TEST_PACK_TEMPLATE.zip")

ASSETS_LIB_DIR = os.path.join(os.path.dirname(__file__), "assets", "lib")

def copy_assets_libs(target_lib_dir: str):
    if not os.path.exists(ASSETS_LIB_DIR):
        print(f"[WARN] assets/lib not found: {ASSETS_LIB_DIR}. Skipping lib copy.")
        return
    os.makedirs(target_lib_dir, exist_ok=True)
    for root, _, files in os.walk(ASSETS_LIB_DIR):
        rel_root = os.path.relpath(root, ASSETS_LIB_DIR)
        dest_root = target_lib_dir if rel_root == "." else os.path.join(target_lib_dir, rel_root)
        os.makedirs(dest_root, exist_ok=True)
        for fn in files:
            shutil.copy2(os.path.join(root, fn), os.path.join(dest_root, fn))
    print(f"[OK] Copied libs from assets/lib to: {target_lib_dir}")

# =========================================================
# (OPTIONAL) EMBEDDED LIB ZIP
# =========================================================
LIB_ZIP_B64 = ""  # optional

def extract_embedded_libs(target_lib_dir: str):
    if not LIB_ZIP_B64.strip():
        return
    data = base64.b64decode(LIB_ZIP_B64)
    with zipfile.ZipFile(io.BytesIO(data), "r") as z:
        names = z.namelist()
        has_lib_folder = any(n.startswith("lib/") for n in names)
        for n in names:
            if n.endswith("/"):
                continue
            if has_lib_folder:
                if not n.startswith("lib/"):
                    continue
                rel = n[len("lib/"):]
            else:
                rel = n
            out_path = os.path.join(target_lib_dir, rel)
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with z.open(n) as src, open(out_path, "wb") as dst:
                shutil.copyfileobj(src, dst)

# =========================================================
# UTILITIES
# =========================================================
def make_dir(p): os.makedirs(p, exist_ok=True)

# FIX: avoid UnicodeEncodeError when ENC can't represent some chars
# xmlcharrefreplace turns them into &#NNNN; (safe for XML)
def write_text(path, content, encoding=ENC):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding=encoding, newline="\n", errors="xmlcharrefreplace") as f:
        f.write(content)

def read_text_guess(path: str):
    for enc in ("utf-8", "ISO-8859-1", "cp1252"):
        try:
            with open(path, "r", encoding=enc) as f:
                return f.read()
        except Exception:
            pass
    return None

def xml_escape(s) -> str:
    if s is None:
        return ""
    s = str(s)
    return (s.replace("&", "&amp;")
             .replace("<", "<")
             .replace(">", ">")
             .replace('"', "&quot;")
             .replace("'", "&apos;"))

def safe_folder_name(name: str) -> str:
    bad = ['\\', '/', ':', '*', '?', '"', '<', '>', '|']
    for b in bad:
        name = name.replace(b, "_")
    return name.strip()

def global_variables_config_xml(global_dict: dict) -> str:
    rows = []
    for k, v in global_dict.items():
        rows.append(f"""        <config>
            <key>{xml_escape(k)}</key>
            <value>{xml_escape("" if v is None else v)}</value>
        </config>""")
    return f'''<?xml version="1.0" encoding="{ENC}" standalone="yes"?>
<globalVariablesConfig>
    <globalVariables>
{chr(10).join(rows)}
    </globalVariables>
</globalVariablesConfig>
'''

def replace_global_var_value_in_xml(xml_text: str, key: str, new_value: str) -> str:
    key_esc = re.escape(key)
    pattern = re.compile(
        rf'(<config>\s*<key>\s*{key_esc}\s*</key>\s*<value>)(.*?)(</value>\s*</config>)',
        flags=re.IGNORECASE | re.DOTALL
    )

    def _repl(m):
        return m.group(1) + xml_escape(new_value) + m.group(3)

    if pattern.search(xml_text):
        return pattern.sub(_repl, xml_text, count=1)

    insertion = f"""
        <config>
            <key>{xml_escape(key)}</key>
            <value>{xml_escape(new_value)}</value>
        </config>"""

    if "</globalVariables>" in xml_text:
        return xml_text.replace("</globalVariables>", insertion + "\n    </globalVariables>", 1)

    return xml_text + insertion

# =========================================================
# node.info helpers (File comparisons)
# =========================================================
def suite_node_info(field_name: str, suite_name: str, description: str, sort_index: int = 0) -> str:
    return f'''<?xml version="1.0" encoding="{ENC}" standalone="yes"?>
<node>
    <fieldName>{xml_escape(field_name)}</fieldName>
    <simpleProperty>
        <fieldName>description</fieldName>
        <type>STRING</type>
        <value>{description}</value>
    </simpleProperty>
    <simpleProperty>
        <fieldName>threads</fieldName>
        <type>STRING</type>
        <value>0</value>
    </simpleProperty>
    <simpleProperty>
        <fieldName>parallel</fieldName>
        <type>BOOLEAN</type>
        <value>false</value>
    </simpleProperty>
    <listItem>
        <fieldName>testConfigs</fieldName>
        <autoDiscover>true</autoDiscover>
        <autoDiscoverFieldType>CHILD_NODE</autoDiscoverFieldType>
    </listItem>
    <simpleProperty>
        <fieldName>exclude</fieldName>
        <type>BOOLEAN</type>
        <value>false</value>
    </simpleProperty>
    <simpleProperty>
        <fieldName>name</fieldName>
        <type>STRING</type>
        <value>{xml_escape(suite_name)}</value>
    </simpleProperty>
    <type>Suite Config</type>
    <sortIndex>{sort_index}</sortIndex>
</node>
'''

def test_node_info(test_name: str, sort_index: int, class_id: str) -> str:
    return f'''<?xml version="1.0" encoding="{ENC}" standalone="yes"?>
<node>
    <fieldName>testConfigs</fieldName>
    <simpleProperty>
        <fieldName>notFaultTolerant</fieldName>
        <type>BOOLEAN</type>
        <value>false</value>
    </simpleProperty>
    <simpleProperty>
        <fieldName>clones</fieldName>
        <type>INTEGER</type>
        <value>0</value>
    </simpleProperty>
    <simpleProperty>
        <fieldName>className</fieldName>
        <type>STRING</type>
        <value>murex.scenario.client.tools.comparator.file.FileComparisonTest</value>
    </simpleProperty>
    <simpleProperty>
        <fieldName>description</fieldName>
        <type>STRING</type>
        <value>&#8226;\t{xml_escape(test_name)} &#8211; PKs: </value>
    </simpleProperty>
    <simpleProperty>
        <fieldName>classId</fieldName>
        <type>STRING</type>
        <value>{xml_escape(class_id)}</value>
    </simpleProperty>
    <simpleProperty>
        <fieldName>notSuiteFaultTolerant</fieldName>
        <type>BOOLEAN</type>
        <value>false</value>
    </simpleProperty>
    <attachmentProperty>
        <fieldName>config</fieldName>
        <type>JAXB</type>
        <attachmentName>config.xml</attachmentName>
    </attachmentProperty>
    <simpleProperty>
        <fieldName>exclude</fieldName>
        <type>BOOLEAN</type>
        <value>false</value>
    </simpleProperty>
    <simpleProperty>
        <fieldName>name</fieldName>
        <type>STRING</type>
        <value>{xml_escape(test_name)}</value>
    </simpleProperty>
    <type>Test Config</type>
    <sortIndex>{sort_index}</sortIndex>
</node>
'''

# =========================================================
# FileComparison config.xml (UPDATED: PKs -> objectIDConfig)
# =========================================================
def file_comparison_config_xml(
    test_filename: str,
    expected_path: str,
    reached_path: str,
    fields: List[str],
    primary_keys: Optional[List[str]] = None
) -> str:
    test_filename_x = xml_escape(test_filename)
    expected_path_x = xml_escape(expected_path)
    reached_path_x = xml_escape(reached_path)

    pk_set = set([str(x).strip() for x in (primary_keys or []) if str(x).strip()])

    base, ext = os.path.splitext(test_filename)
    ext = ext.lstrip(".")
    file_match_regex_x = xml_escape(f"({base}).*\\.{ext}$") if ext else ""

    def configs_by_path(fields_):
        out = []
        for f in fields_:
            fx = xml_escape(f)
            is_pk = (str(f).strip() in pk_set)
            if is_pk:
                out.append(f'''                                        <entry>
                                            <key xsi:type="xs:string" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">{fx}</key>
                                            <value xsi:type="pathComparisonConfig" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                                                <comparisonTypeConfig xsi:type="objectIDConfig"/>
                                            </value>
                                        </entry>''')
            else:
                out.append(f'''                                        <entry>
                                            <key xsi:type="xs:string" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">{fx}</key>
                                            <value xsi:type="pathComparisonConfig" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                                                <comparisonTypeConfig xsi:type="pointerTypeConfig"/>
                                                <toleranceConfig xsi:type="pointerToleranceConfig"/>
                                            </value>
                                        </entry>''')
        return "\n".join(out)

    def path_mappings(fields_):
        out = []
        for f in fields_:
            fx = xml_escape(f)
            out.append(f'''                                        <entry>
                                            <key xsi:type="xs:string" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">{fx}</key>
                                            <value xsi:type="xs:string" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">{fx}</value>
                                        </entry>''')
        return "\n".join(out)

    ordered_paths = "\n".join([f"                                        <orderedPath>{xml_escape(f)}</orderedPath>" for f in fields])

    return f'''<?xml version="1.0" encoding="{ENC}" standalone="yes"?>
<config xmlns:ns2="objects.runtime.murex">
    <properties>
        <complex-property name="expectedProviderReference">
            <type>murex.runtime.objects.ResourceProviderReference</type>
            <resourceType>Pool</resourceType>
            <readOnly>false</readOnly>
            <xmlValue>
                <xsdType>
                    <name>ResourceProviderReference</name>
                    <namespace>murex.runtime.objects</namespace>
                    <groupId>-1</groupId>
                    <type>murex.runtime.objects.ResourceProviderReference</type>
                </xsdType>
            </xmlValue>
            <parameterId>270934496144173541</parameterId>
        </complex-property>
        <complex-property name="reachedProviderReference">
            <type>murex.runtime.objects.ResourceProviderReference</type>
            <resourceType>Pool</resourceType>
            <readOnly>false</readOnly>
            <xmlValue>
                <xsdType>
                    <name>ResourceProviderReference</name>
                    <namespace>murex.runtime.objects</namespace>
                    <groupId>-1</groupId>
                    <type>murex.runtime.objects.ResourceProviderReference</type>
                </xsdType>
            </xmlValue>
            <parameterId>270934496144173541</parameterId>
        </complex-property>
        <complex-property name="expectedFtpConfiguration">
            <type>murex.application.test.rt.bridge.app.resource.ResourceReference</type>
            <readOnly>false</readOnly>
            <xmlValue>
                <xsdType>
                    <name>ResourceReference</name>
                    <namespace>murex.application.test.rt.bridge.app.resource</namespace>
                    <groupId>-1</groupId>
                    <type>murex.application.test.rt.bridge.app.resource.ResourceReference</type>
                </xsdType>
            </xmlValue>
            <parameterId>4165779022442054221</parameterId>
        </complex-property>
        <complex-property name="reachedFtpConfiguration">
            <type>murex.application.test.rt.bridge.app.resource.ResourceReference</type>
            <readOnly>false</readOnly>
            <xmlValue>
                <xsdType>
                    <name>ResourceReference</name>
                    <namespace>murex.application.test.rt.bridge.app.resource</namespace>
                    <groupId>-1</groupId>
                    <type>murex.application.test.rt.bridge.app.resource.ResourceReference</type>
                </xsdType>
            </xmlValue>
            <parameterId>4165779022442054221</parameterId>
        </complex-property>
        <complex-property name="EXPECTED_DB">
            <type>murex.application.test.rt.bridge.app.db.DatabaseReference</type>
            <readOnly>false</readOnly>
            <xmlValue>
                <xsdType>
                    <name>DatabaseReference</name>
                    <namespace>murex.application.test.rt.bridge.app.db</namespace>
                    <groupId>-1</groupId>
                    <type>murex.application.test.rt.bridge.app.db.DatabaseReference</type>
                </xsdType>
            </xmlValue>
            <parameterId>4000636150563545400</parameterId>
        </complex-property>
        <complex-property name="REACHED_DB">
            <type>murex.application.test.rt.bridge.app.db.DatabaseReference</type>
            <readOnly>false</readOnly>
            <xmlValue>
                <xsdType>
                    <name>DatabaseReference</name>
                    <namespace>murex.application.test.rt.bridge.app.db</namespace>
                    <groupId>-1</groupId>
                    <type>murex.application.test.rt.bridge.app.db.DatabaseReference</type>
                </xsdType>
            </xmlValue>
            <parameterId>4000636150563545400</parameterId>
        </complex-property>
        <simple-property name="path">
            <type>java.lang.String</type>
            <readOnly>false</readOnly>
        </simple-property>
        <context-property name="expectedFiles">
            <type>murex.scenario.core.tools.file.FilePath</type>
            <readOnly>false</readOnly>
        </context-property>
        <context-property name="reachedFiles">
            <type>murex.scenario.core.tools.file.FilePath</type>
            <readOnly>false</readOnly>
        </context-property>
    </properties>

    <metaDataConfig>
        <methodAsserterConfigs>
            <methodAsserterConfigs>
                <methodAsserterConfig methodName="compareFiles">
                    <assertionIdsConfigs>
                        <assertionIdConfig assertionId="{test_filename_x}">
                            <csvAsserterConfig id="{test_filename_x}" skipped="false" inverted="false" hasReference="false">
                                <exportDirPath>File Comparison Results</exportDirPath>

                                <expectedFileAsserterConfig>
                                    <fileLocationType>LOCAL</fileLocationType>
                                    <filePath>{expected_path_x}</filePath>
                                    <saveExcelAsCSV>false</saveExcelAsCSV>
                                    <attachmentPropertyConfig name="Expected File Attachment">
                                        <type></type>
                                        <readOnly>false</readOnly>
                                    </attachmentPropertyConfig>
                                </expectedFileAsserterConfig>

                                <reachedFileAsserterConfig>
                                    <fileLocationType>LOCAL</fileLocationType>
                                    <filePath>{reached_path_x}</filePath>
                                    <saveExcelAsCSV>false</saveExcelAsCSV>
                                    <attachmentPropertyConfig name="Reached File Attachment">
                                        <type></type>
                                        <readOnly>false</readOnly>
                                    </attachmentPropertyConfig>
                                </reachedFileAsserterConfig>

                                <fileMatchRegex>{file_match_regex_x}</fileMatchRegex>
                                <ignoreLeftoverFiles>false</ignoreLeftoverFiles>

                                <comparatorConfig>
                                    <configsByPath>
{configs_by_path(fields)}
                                    </configsByPath>

                                    <pathMappings>
{path_mappings(fields)}
                                    </pathMappings>

                                    <orderedPaths>
{ordered_paths}
                                    </orderedPaths>

                                    <aggregateKeys>false</aggregateKeys>
                                    <aggregateOperation>SUM</aggregateOperation>
                                    <trimKeys>true</trimKeys>
                                    <ignoreCaseForKeys>false</ignoreCaseForKeys>
                                    <skipSorting>false</skipSorting>
                                    <doNotFailOnColumnNotFound>false</doNotFailOnColumnNotFound>
                                    <compareAllColumns>false</compareAllColumns>
                                </comparatorConfig>

                                <ignoreAddedRows>false</ignoreAddedRows>
                                <ignoreRemovedRows>false</ignoreRemovedRows>
                                <includeMatchingRows>true</includeMatchingRows>
                                <exportFileExtension>.xlsx</exportFileExtension>
                            </csvAsserterConfig>
                        </assertionIdConfig>
                    </assertionIdsConfigs>
                </methodAsserterConfig>
            </methodAsserterConfigs>
        </methodAsserterConfigs>
    </metaDataConfig>
</config>
'''
# =========================================================
# Resolve paths for file comparisons
# =========================================================
def resolve_expected_reached_paths(cfg: dict) -> Tuple[str, str]:
    fc = cfg.get("file_comparison") or {}
    sp = fc.get("source_path")
    tp = fc.get("target_path")
    if sp and tp:
        return str(sp), str(tp)

    env = cfg.get("environment") or {}
    sp = env.get("source_path")
    tp = env.get("target_path")
    if sp and tp:
        return str(sp), str(tp)

    gvars = cfg.get("global_variables") or {}
    sp = gvars.get("SOURCE_PATH")
    tp = gvars.get("TARGET_PATH")
    if sp and tp:
        return str(sp), str(tp)

    sp = env.get("source", "")
    tp = env.get("target", "")
    return str(sp), str(tp)

# =========================================================
# SMOKE BUILD (SAFE: only touch apps/config.xml + globalVariablesConfig.xml)
# =========================================================
def build_smoke_from_assets(cfg: dict, out_dir: str):
    if not os.path.exists(SMOKE_TEMPLATE_ZIP_PATH):
        raise RuntimeError(f"Smoke template not found at: {SMOKE_TEMPLATE_ZIP_PATH}")

    env = (cfg.get("environment") or {}).get("source") or ""
    env = str(env).strip()
    if not env:
        raise RuntimeError("Environment is required for Smoke test.")

    gvars = cfg.get("global_variables") or {}
    required = ["MXUSER", "UserCredentialPassword", "MXCredentialPassword"]
    missing = [k for k in required if str(gvars.get(k, "")).strip() == ""]
    if missing:
        raise RuntimeError(f"Missing mandatory global variable values for Smoke test: {', '.join(missing)}")

    with open(SMOKE_TEMPLATE_ZIP_PATH, "rb") as f:
        data = f.read()

    with tempfile.TemporaryDirectory() as td:
        with zipfile.ZipFile(io.BytesIO(data), "r") as z:
            z.extractall(td)

        roots = [d for d in os.listdir(td) if os.path.isdir(os.path.join(td, d))]
        smoke_roots = [r for r in roots if r.upper().startswith("SMOKE_TEST_")]
        if len(smoke_roots) != 1:
            raise RuntimeError(f"Could not uniquely identify SMOKE_TEST root folder. Found: {smoke_roots} (all roots: {roots})")

        smoke_root_name = smoke_roots[0]
        smoke_root = os.path.join(td, smoke_root_name)

        # 1) Replace env ONLY in apps/config.xml
        apps_cfg = os.path.join(smoke_root, "apps", "config.xml")
        if os.path.exists(apps_cfg):
            txt = read_text_guess(apps_cfg)
            if txt is not None and "MXTEST_O9_ENV" in txt:
                write_text(apps_cfg, txt.replace("MXTEST_O9_ENV", env), encoding=ENC)

        # 2) Update ONLY the 3 values in existing template globalVariablesConfig.xml
        gv_path = os.path.join(smoke_root, "config", "GlobalTestConfiguration", "GlobalVariables", "globalVariablesConfig.xml")
        if not os.path.exists(gv_path):
            raise RuntimeError(f"globalVariablesConfig.xml not found in template at: {gv_path}")

        orig_xml = read_text_guess(gv_path)
        if orig_xml is None:
            raise RuntimeError("Could not read globalVariablesConfig.xml as text")

        updated = orig_xml
        updated = replace_global_var_value_in_xml(updated, "MXUSER", gvars.get("MXUSER", ""))
        updated = replace_global_var_value_in_xml(updated, "UserCredentialPassword", gvars.get("UserCredentialPassword", ""))
        updated = replace_global_var_value_in_xml(updated, "MXCredentialPassword", gvars.get("MXCredentialPassword", ""))

        write_text(gv_path, updated, encoding=ENC)

        # 3) Output contains ONLY SMOKE_TEST folder, renamed with env
        safe_env = env.strip().replace(" ", "_")
        new_smoke_root_name = f"SMOKE_TEST_{safe_env}"

        if os.path.exists(out_dir):
            shutil.rmtree(out_dir)
        os.makedirs(out_dir, exist_ok=True)

        shutil.copytree(smoke_root, os.path.join(out_dir, new_smoke_root_name))

# =========================================================
# MAIN
# =========================================================
def main():
    config_file = sys.argv[1]
    with open(config_file, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    pkg_name = cfg["package"]["name"]
    pkg_version = cfg["package"]["version"]
    pkg_dir = f"{pkg_name}_{pkg_version}"

    test_suite = (cfg.get("test_suite") or "").strip()

    # Smoke mode
    if test_suite == "Smoke test":
        build_smoke_from_assets(cfg, pkg_dir)
        print(f"[OK] Smoke package generated: {pkg_dir}")
        return

    # -----------------------------
    # File comparisons mode
    # -----------------------------
    if os.path.exists(pkg_dir):
        shutil.rmtree(pkg_dir)

    for d in ["apps", "lib", "reference", "runners", "config"]:
        make_dir(os.path.join(pkg_dir, d))

    # Copy local project libs first
    copy_assets_libs(os.path.join(pkg_dir, "lib"))

    # Then optional embedded libs
    extract_embedded_libs(os.path.join(pkg_dir, "lib"))

    # apps/config.xml
    write_text(os.path.join(pkg_dir, "apps", "config.xml"),
               f'''<?xml version="1.0" encoding="{ENC}" standalone="yes"?>
<appConfigs>
</appConfigs>
''', encoding=ENC)

    # config/GlobalTestConfiguration
    conf_dir = os.path.join(pkg_dir, "config")
    gtc = os.path.join(conf_dir, "GlobalTestConfiguration")
    for sub in ["DisplayServer", "ExecutionProperties", "ExplanationPackages", "GlobalVariables", "TestCoverage"]:
        make_dir(os.path.join(gtc, sub))

    # GlobalVariables
    gvars = cfg.get("global_variables", {})
    write_text(os.path.join(gtc, "GlobalVariables", "globalVariablesConfig.xml"),
               global_variables_config_xml(gvars), encoding=ENC)
    write_text(os.path.join(gtc, "GlobalVariables", "node.info"),
               f'''<?xml version="1.0" encoding="{ENC}" standalone="yes"?>
<node>
    <fieldName>globalVariables</fieldName>
    <attachmentProperty>
        <fieldName>globalVariablesConfig</fieldName>
        <type>JAXB</type>
        <attachmentName>globalVariablesConfig.xml</attachmentName>
    </attachmentProperty>
    <type>Global Variables Config</type>
    <sortIndex>0</sortIndex>
</node>
''', encoding=ENC)

    # Otros node.info básicos
    write_text(os.path.join(gtc, "DisplayServer", "node.info"),
               f'''<?xml version="1.0" encoding="{ENC}" standalone="yes"?>
<node>
    <fieldName>displayServerConfigOtmNode</fieldName>
    <simpleProperty>
        <fieldName>displayServerConfigName</fieldName>
        <type>STRING</type>
        <value>Default</value>
    </simpleProperty>
    <type>Display Server</type>
    <sortIndex>0</sortIndex>
</node>
''', encoding=ENC)

    write_text(os.path.join(gtc, "ExplanationPackages", "node.info"),
               f'''<?xml version="1.0" encoding="{ENC}" standalone="yes"?>
<node>
    <fieldName>explanationPackages</fieldName>
    <listItem>
        <fieldName>packages</fieldName>
        <autoDiscover>false</autoDiscover>
    </listItem>
    <type>Explanation Packages</type>
    <sortIndex>0</sortIndex>
</node>
''', encoding=ENC)

    write_text(os.path.join(gtc, "ExecutionProperties", "executionPropertiesConfig.xml"),
               f'''<?xml version="1.0" encoding="{ENC}" standalone="yes"?>
<executionPropertiesConfig>
    <parallelExecutionConfig>
        <parallelTestsCount>0</parallelTestsCount>
        <enabled>false</enabled>
    </parallelExecutionConfig>
</executionPropertiesConfig>
''', encoding=ENC)

    write_text(os.path.join(gtc, "ExecutionProperties", "node.info"),
               f'''<?xml version="1.0" encoding="{ENC}" standalone="yes"?>
<node>
    <fieldName>executionProperties</fieldName>
    <attachmentProperty>
        <fieldName>executionPropertiesConfig</fieldName>
        <type>JAXB</type>
        <attachmentName>executionPropertiesConfig.xml</attachmentName>
    </attachmentProperty>
    <type>Global Execution Config</type>
    <sortIndex>0</sortIndex>
</node>
''', encoding=ENC)

    write_text(os.path.join(gtc, "TestCoverage", "testCoverageConfig.xml"),
               f'''<?xml version="1.0" encoding="{ENC}" standalone="yes"?>
<testCoverageConfig>
    <testCoverages/>
</testCoverageConfig>
''', encoding=ENC)

    write_text(os.path.join(gtc, "TestCoverage", "node.info"),
               f'''<?xml version="1.0" encoding="{ENC}" standalone="yes"?>
<node>
    <fieldName>testCoverageOtmNode</fieldName>
    <attachmentProperty>
        <fieldName>testCoverageConfig</fieldName>
        <type>JAXB</type>
        <attachmentName>testCoverageConfig.xml</attachmentName>
    </attachmentProperty>
    <type>Test Coverage Config</type>
    <sortIndex>0</sortIndex>
</node>
''', encoding=ENC)

    # config/node.info
    write_text(os.path.join(conf_dir, "node.info"),
               f'''<?xml version="1.0" encoding="{ENC}" standalone="yes"?>
<node>
    <childNode lazy="false">
        <fieldName>displayServerConfigOtmNode</fieldName>
        <name>GlobalTestConfiguration/DisplayServer</name>
    </childNode>
    <childNode lazy="false">
        <fieldName>globalVariables</fieldName>
        <name>GlobalTestConfiguration/GlobalVariables</name>
    </childNode>
    <listItem>
        <fieldName>tolerancePackages</fieldName>
        <autoDiscover>false</autoDiscover>
    </listItem>
    <childNode lazy="false">
        <fieldName>explanationPackages</fieldName>
        <name>GlobalTestConfiguration/ExplanationPackages</name>
    </childNode>
    <childNode lazy="false">
        <fieldName>testCoverageOtmNode</fieldName>
        <name>GlobalTestConfiguration/TestCoverage</name>
    </childNode>
    <childNode lazy="false">
        <fieldName>testEngineConfig</fieldName>
        <name>TestPackageConfig</name>
    </childNode>
    <mapItem>
        <fieldName>properties</fieldName>
    </mapItem>
    <childNode lazy="false">
        <fieldName>executionProperties</fieldName>
        <name>GlobalTestConfiguration/ExecutionProperties</name>
    </childNode>
    <type>New Config Node</type>
    <sortIndex>0</sortIndex>
</node>
''', encoding=ENC)

    # TestPackageConfig/node.info
    tpc_dir = os.path.join(conf_dir, "TestPackageConfig")
    make_dir(tpc_dir)
    write_text(os.path.join(tpc_dir, "node.info"),
               f'''<?xml version="1.0" encoding="{ENC}" standalone="yes"?>
<node>
    <fieldName>testEngineConfig</fieldName>
    <childNode lazy="false">
        <fieldName>subSequenceConfigs</fieldName>
        <name>_SubSequences</name>
    </childNode>
    <childNode lazy="false">
        <fieldName>suiteConfig</fieldName>
        <name>Root Suite</name>
    </childNode>
    <childNode lazy="false">
        <fieldName>recordComparisonConfigs</fieldName>
        <name>_PerformanceAnalytics</name>
    </childNode>
    <childNode lazy="false">
        <fieldName>resourceProviderConfigs</fieldName>
        <name>_Resources</name>
    </childNode>
    <type>Test Engine Config</type>
    <sortIndex>0</sortIndex>
</node>
''', encoding=ENC)

    # Root Suite node.info
    root_suite_dir = os.path.join(tpc_dir, "Root Suite")
    make_dir(root_suite_dir)
    root_desc = f"MXtest Package  &#8211; {xml_escape(pkg_name)}"
    write_text(os.path.join(root_suite_dir, "node.info"),
               suite_node_info("suiteConfig", "Root Suite", root_desc, 0), encoding=ENC)

    # File Comparisons suite node.info
    fc_dir = os.path.join(root_suite_dir, "File Comparisons")
    make_dir(fc_dir)
    write_text(os.path.join(fc_dir, "node.info"),
               suite_node_info("testConfigs", "File Comparisons", root_desc, 0), encoding=ENC)

    expected_path, reached_path = resolve_expected_reached_paths(cfg)
    DAY0_CLASS_ID = "88a78a32-254c-43b2-b315-6a923dcb9a28"

    sort_idx = 1
    for fobj in cfg.get("files", []):
        test_filename = (fobj.get("name") or "").strip()
        if not test_filename:
            continue

        fields = fobj.get("fields", [])
        if isinstance(fields, str):
            fields = [x.strip() for x in fields.split(",") if x.strip()]

        use_pk = bool(fobj.get("use_primary_keys", False))
        primary_keys = fobj.get("primary_keys", []) if use_pk else []
        if isinstance(primary_keys, str):
            primary_keys = [x.strip() for x in primary_keys.split(",") if x.strip()]

        test_folder = safe_folder_name(test_filename.replace(".", "_"))
        test_dir = os.path.join(fc_dir, test_folder)
        make_dir(test_dir)

        write_text(
            os.path.join(test_dir, "config.xml"),
            file_comparison_config_xml(
                test_filename,
                expected_path,
                reached_path,
                fields,
                primary_keys=primary_keys
            ),
            encoding=ENC
        )

        write_text(os.path.join(test_dir, "node.info"),
                   test_node_info(test_filename, sort_idx, DAY0_CLASS_ID),
                   encoding=ENC)

        sort_idx += 1

    print(f"[OK] Package generated: {pkg_dir}")
    print(f"Expected filePath  : {expected_path}")
    print(f"Reached filePath   : {reached_path}")

if __name__ == "__main__":
    main()