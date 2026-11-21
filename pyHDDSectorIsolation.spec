# -*- mode: python ; coding: utf-8 -*-
# VERSION=2
# https://github.com/ageorge95/ag95_repo_base


def merge_toc(*tocs):
    """Merge TOCs from both analyses, keeping one entry per destination path."""
    seen = set()
    merged = []
    for toc in tocs:
        for entry in toc:
            destination = entry[0]
            if destination not in seen:
                seen.add(destination)
                merged.append(entry)
    return merged


a1 = Analysis(
    ['module1.py'],
    pathex=[],
    binaries=[],
    datas=[('icon.ico', '.'),
           ('version.txt', '.')],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
a2 = Analysis(
    ['module2.py'],
    pathex=[],
    binaries=[],
    datas=[('icon.ico', '.'),
           ('version.txt', '.')],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)

pyz1 = PYZ(a1.pure)
pyz2 = PYZ(a2.pure)

exe1 = EXE(
    pyz1,
    a1.scripts,
    [],
    exclude_binaries=True,
    name='module1',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['icon.ico'],
)
exe2 = EXE(
    pyz2,
    a2.scripts,
    [],
    exclude_binaries=True,
    name='module2',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['icon.ico'],
)

coll = COLLECT(
    exe1,
    exe2,
    merge_toc(a1.binaries, a2.binaries),
    merge_toc(a1.datas, a2.datas),
    strip=False,
    upx=True,
    upx_exclude=[],
    name='pyHDDSectorIsolation',
)
