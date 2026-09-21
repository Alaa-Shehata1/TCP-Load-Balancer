#!/usr/bin/env python3
"""Render captured terminal sessions as PNG screenshots.

Usage: python3 scripts/render_shots.py
Reads docs/screenshots/*.session (prompt lines start with '$ '),
writes docs/screenshots/*.png. Regenerate after changing demo output.
"""
import os
from PIL import Image, ImageDraw, ImageFont

BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "docs", "screenshots")
FONT_PATH = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"
FONT_SIZE = 15
PAD = 16
TITLE_H = 34
BG = (30, 30, 46)
FG = (205, 214, 244)
PROMPT_FG = (166, 227, 161)
TITLE_FG = (147, 153, 178)
DOTS = [(243, 139, 168), (249, 226, 175), (166, 227, 161)]

SHOTS = {
    "demo-rotation": "lb — round-robin",
    "demo-failover": "lb — failover after docker stop server-b",
    "demo-metrics": "lb — Prometheus metrics",
}


def render(session_path, title, out_path):
    with open(session_path) as f:
        lines = f.read().rstrip("\n").split("\n")
    font = ImageFont.truetype(FONT_PATH, FONT_SIZE)
    ascent, descent = font.getmetrics()
    line_h = ascent + descent + 4
    tmp = ImageDraw.Draw(Image.new("RGB", (10, 10)))
    width = int(max([tmp.textlength(title, font=font)] + [tmp.textlength(ln, font=font) for ln in lines])) + PAD * 2 + 60
    height = TITLE_H + line_h * len(lines) + PAD
    img = Image.new("RGB", (width, height), BG)
    d = ImageDraw.Draw(img)
    for i, (x, color) in enumerate(zip((14, 32, 50), DOTS)):
        d.ellipse([x, 12, x + 10, 22], fill=color)
    d.text((70, 8), title, font=font, fill=TITLE_FG)
    y = TITLE_H
    for ln in lines:
        if ln.startswith("$ "):
            d.text((PAD, y), "$", font=font, fill=PROMPT_FG)
            d.text((PAD + tmp.textlength("$", font=font), y), ln[1:], font=font, fill=FG)
        else:
            d.text((PAD, y), ln, font=font, fill=FG)
        y += line_h
    img.save(out_path)
    print(f"wrote {out_path} ({width}x{height})")


def main():
    for name, title in SHOTS.items():
        render(
            os.path.join(BASE, name + ".session"),
            title,
            os.path.join(BASE, name + ".png"),
        )


if __name__ == "__main__":
    main()
