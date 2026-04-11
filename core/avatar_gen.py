"""Generate Nobel-style medallion SVG avatars for laureates."""

ERA_COLORS = {
    "foundation": {"bg": "#C9A84C", "border": "#8B6914", "text": "#3D2B00"},
    "systems":    {"bg": "#A8B5C0", "border": "#5A6B7A", "text": "#1A2530"},
    "internet":   {"bg": "#CD7F4A", "border": "#8B4513", "text": "#2D1100"},
    "modern":     {"bg": "#B0C4D8", "border": "#4A6E8C", "text": "#0A2540"},
}


def generate_avatar_svg(initials: str, era: str, year: int, size: int = 64) -> str:
    colors = ERA_COLORS.get(era, ERA_COLORS["foundation"])
    font_size = int(size * 0.35)
    year_size = int(size * 0.14)
    cx = size // 2
    cy = size // 2
    r = size // 2 - 2

    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" viewBox="0 0 {size} {size}">
  <defs>
    <radialGradient id="g_{initials}{year}" cx="40%" cy="35%">
      <stop offset="0%" stop-color="{colors['bg']}" stop-opacity="1"/>
      <stop offset="100%" stop-color="{colors['border']}" stop-opacity="1"/>
    </radialGradient>
  </defs>
  <circle cx="{cx}" cy="{cx}" r="{r}" fill="url(#g_{initials}{year})" stroke="{colors['border']}" stroke-width="2"/>
  <circle cx="{cx}" cy="{cx}" r="{r - 4}" fill="none" stroke="{colors['bg']}" stroke-width="1" opacity="0.5"/>
  <text x="{cx}" y="{cy - 2}" text-anchor="middle" dominant-baseline="central"
        font-family="Georgia, serif" font-size="{font_size}" font-weight="bold"
        fill="{colors['text']}" opacity="0.9">{initials}</text>
  <text x="{cx}" y="{cy + int(size*0.27)}" text-anchor="middle"
        font-family="Georgia, serif" font-size="{year_size}"
        fill="{colors['text']}" opacity="0.6">{year}</text>
</svg>'''
