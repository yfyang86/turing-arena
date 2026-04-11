#!/usr/bin/env python3
"""
Slice a 9×9 Turing laureate portrait grid into individual avatar images.

Usage:
    python utils/slice_avatars.py <grid_image> [--output-dir static/img/avatars]

Input:  1080×1080 PNG with 81 portraits in 9×9 grid (left→right, top→bottom, 1966→2025)
Output: 81 PNG files named by laureate slug, e.g. donald-e-knuth.png

Each cell is 120×120px. Portraits are cropped to a centered square and resized
to multiple sizes for use as chat avatars and info cards.
"""

from __future__ import annotations
import argparse
import sys
from pathlib import Path

try:
    from PIL import Image
except ImportError:
    print("Error: Pillow is required. Install with: pip install Pillow")
    sys.exit(1)

# Grid position (row-major, 0-indexed) → laureate slug
# Exactly matches the user's list: left-top to right-bottom
GRID_ORDER: list[str] = [
    # Row 0: 1966–1974
    "alan-j-perlis",          # 1966
    "maurice-v-wilkes",       # 1967
    "richard-w-hamming",      # 1968
    "marvin-minsky",          # 1969
    "james-h-wilkinson",      # 1970
    "john-mccarthy",          # 1971
    "edsger-w-dijkstra",      # 1972
    "charles-w-bachman",      # 1973
    "donald-e-knuth",         # 1974
    # Row 1: 1975–1981
    "allen-newell",           # 1975
    "herbert-a-simon",        # 1975
    "michael-o-rabin",        # 1976
    "dana-s-scott",           # 1976
    "john-backus",            # 1977
    "robert-w-floyd",         # 1978
    "kenneth-e-iverson",      # 1979
    "car-hoare",              # 1980
    "edgar-f-codd",           # 1981
    # Row 2: 1982–1988
    "stephen-a-cook",         # 1982
    "ken-thompson",           # 1983
    "dennis-ritchie",         # 1983
    "niklaus-wirth",          # 1984
    "richard-m-karp",         # 1985
    "john-hopcroft",          # 1986
    "robert-tarjan",          # 1986
    "john-cocke",             # 1987
    "ivan-sutherland",        # 1988
    # Row 3: 1989–1995
    "william-kahan",          # 1989
    "fernando-j-corbato",     # 1990
    "robin-milner",           # 1991
    "butler-w-lampson",       # 1992
    "juris-hartmanis",        # 1993
    "richard-e-stearns",      # 1993
    "edward-a-feigenbaum",    # 1994
    "raj-reddy",              # 1994
    "manuel-blum",            # 1995
    # Row 4: 1996–2002
    "amir-pnueli",            # 1996
    "douglas-engelbart",      # 1997
    "jim-gray",               # 1998
    "frederick-p-brooks",     # 1999
    "andrew-chi-chih-yao",    # 2000
    "ole-johan-dahl",         # 2001
    "kristen-nygaard",        # 2001
    "ronald-l-rivest",        # 2002
    "adi-shamir",             # 2002
    # Row 5: 2002–2007
    "leonard-m-adleman",      # 2002
    "alan-kay",               # 2003
    "vinton-g-cerf",          # 2004
    "robert-e-kahn",          # 2004
    "peter-naur",             # 2005
    "frances-e-allen",        # 2006
    "edmund-m-clarke",        # 2007
    "e-allen-emerson",        # 2007
    "joseph-sifakis",         # 2007
    # Row 6: 2008–2015
    "barbara-liskov",         # 2008
    "charles-p-thacker",      # 2009
    "leslie-valiant",         # 2010
    "judea-pearl",            # 2011
    "shafi-goldwasser",       # 2012
    "silvio-micali",          # 2012
    "leslie-lamport",         # 2013
    "michael-stonebraker",    # 2014
    "whitfield-diffie",       # 2015
    # Row 7: 2015–2019
    "martin-hellman",         # 2015
    "tim-berners-lee",        # 2016
    "john-l-hennessy",        # 2017
    "david-a-patterson",      # 2017
    "yoshua-bengio",          # 2018
    "geoffrey-hinton",        # 2018
    "yann-le-cun",            # 2018
    "edwin-catmull",          # 2019
    "patrick-hanrahan",       # 2019
    # Row 8: 2020–2025
    "alfred-v-aho",           # 2020
    "jeffrey-d-ullman",       # 2020
    "jack-dongarra",          # 2021
    "bob-metcalfe",           # 2022
    "avi-wigderson",          # 2023
    "andrew-g-barto",         # 2024
    "richard-s-sutton",       # 2024
    "charles-h-bennett",      # 2025
    "gilles-brassard",        # 2025
]

assert len(GRID_ORDER) == 81, f"Expected 81 entries, got {len(GRID_ORDER)}"

# Output sizes
SIZES = {
    "full": 120,   # original cell size
    "96": 96,      # info card
    "64": 64,      # chat avatar large
    "32": 32,      # chat avatar small / left panel
}


def slice_grid(image_path: str, output_dir: str, grid_size: int = 9):
    img = Image.open(image_path)
    w, h = img.size
    cell_w = w // grid_size
    cell_h = h // grid_size

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    count = 0
    for idx, slug in enumerate(GRID_ORDER):
        row = idx // grid_size
        col = idx % grid_size
        x0 = col * cell_w
        y0 = row * cell_h
        x1 = x0 + cell_w
        y1 = y0 + cell_h

        cell = img.crop((x0, y0, x1, y1))

        # Crop to portrait area: top has year label, bottom has name/achievement
        pw, ph = cell.size
        portrait_top = int(ph * 0.22)     # skip year label fully
        portrait_bottom = int(ph * 0.74)  # stop before name text
        portrait = cell.crop((2, portrait_top, pw - 2, portrait_bottom))

        # Make square (center-crop the wider dimension)
        pw2, ph2 = portrait.size
        side = min(pw2, ph2)
        left = (pw2 - side) // 2
        top = (ph2 - side) // 2
        square = portrait.crop((left, top, left + side, top + side))

        # Save at multiple sizes
        for suffix, size in SIZES.items():
            resized = square.resize((size, size), Image.LANCZOS)
            if suffix == "full":
                resized.save(out / f"{slug}.png", "PNG", optimize=True)
            else:
                resized.save(out / f"{slug}-{suffix}.png", "PNG", optimize=True)

        count += 1
        if count <= 3 or count % 20 == 0:
            print(f"  [{count:2d}/81] {slug}: {cell_w}×{cell_h} → portrait → square → saved")

    print(f"\nDone: {count} avatars saved to {out}/")
    print(f"Files per laureate: {len(SIZES)} sizes ({', '.join(f'{s}px' for s in SIZES.values())})")
    print(f"Total files: {count * len(SIZES)}")


def main():
    parser = argparse.ArgumentParser(description="Slice Turing laureate portrait grid into individual avatars")
    parser.add_argument("image", help="Path to the 9×9 grid image (1080×1080)")
    parser.add_argument("--output-dir", "-o", default="static/img/avatars",
                        help="Output directory (default: static/img/avatars)")
    parser.add_argument("--grid-size", "-g", type=int, default=9, help="Grid size (default: 9)")
    args = parser.parse_args()

    print(f"Slicing {args.image} ({args.grid_size}×{args.grid_size}) → {args.output_dir}/")
    slice_grid(args.image, args.output_dir, args.grid_size)


if __name__ == "__main__":
    main()
