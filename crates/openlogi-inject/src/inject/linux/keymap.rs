//! Character → key-stroke resolution through the active XKB layout.
//!
//! `uinput` only speaks evdev keycodes; which character a keycode produces is
//! the compositor's decision, driven by the XKB layout the session runs. So
//! `TypeText` cannot hard-code a US table: on a Spanish layout `@` lives on
//! AltGr+2, on a German one `z` and `y` swap. This module compiles the
//! session's layout with libxkbcommon and inverts it into a `char → stroke`
//! map, so the injected keycodes produce the requested text on that layout.
//!
//! Layout discovery is best-effort and desktop-agnostic: GNOME's
//! `input-sources` settings first (the only source that tracks a live layout
//! switch), then the console default in `/etc/default/keyboard`, then `us`.

use std::collections::{HashMap, HashSet};
use std::process::Command;

use evdev::KeyCode;
use xkbcommon::xkb;

/// Evdev keycodes are XKB keycodes minus this offset (an X11 legacy).
const XKB_EVDEV_OFFSET: u32 = 8;

/// Shift levels resolved per key: plain, Shift, AltGr, Shift+AltGr. Higher
/// levels (Lock-only groups, Level5) are exotic enough to leave to the
/// unicode fallback rather than guess their modifiers.
const LEVEL_MODIFIERS: [&[KeyCode]; 4] = [
    &[],
    &[KeyCode::KEY_LEFTSHIFT],
    &[KeyCode::KEY_RIGHTALT],
    &[KeyCode::KEY_LEFTSHIFT, KeyCode::KEY_RIGHTALT],
];

/// The modifiers and key that produce one character on the active layout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Stroke {
    /// Modifiers held while `key` is pressed, in press order.
    pub mods: Vec<KeyCode>,
    /// The character's key.
    pub key: KeyCode,
}

/// XKB layout + variant names, as `setxkbmap` would take them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct LayoutNames {
    pub layout: String,
    pub variant: String,
}

impl Default for LayoutNames {
    fn default() -> Self {
        Self {
            layout: "us".into(),
            variant: String::new(),
        }
    }
}

impl LayoutNames {
    /// Parse a `layout` or `layout+variant` token, the shape both GNOME's
    /// `input-sources` values and `setxkbmap` accept.
    fn parse(token: &str) -> Self {
        let (layout, variant) = token.split_once('+').unwrap_or((token, ""));
        Self {
            layout: layout.trim().to_string(),
            variant: variant.trim().to_string(),
        }
    }
}

/// The layout the session is most likely typing with right now.
pub(super) fn detect_layout() -> LayoutNames {
    from_gnome_settings()
        .or_else(from_etc_default_keyboard)
        .unwrap_or_default()
}

/// GNOME tracks the active layout in `mru-sources` (most recently used) and
/// the configured list in `sources`; the first `xkb` entry of either wins.
fn from_gnome_settings() -> Option<LayoutNames> {
    ["mru-sources", "sources"].iter().find_map(|key| {
        let output = Command::new("gsettings")
            .args(["get", "org.gnome.desktop.input-sources", key])
            .output()
            .ok()?;
        parse_gnome_sources(&String::from_utf8_lossy(&output.stdout))
    })
}

/// Extract the first `('xkb', '<layout>')` entry of a GVariant source list.
fn parse_gnome_sources(text: &str) -> Option<LayoutNames> {
    let rest = text.split("('xkb', '").nth(1)?;
    let token = rest.split('\'').next()?;
    (!token.is_empty()).then(|| LayoutNames::parse(token))
}

/// The console/X default written by `keyboard-configuration`: the first entry
/// of the comma-separated `XKBLAYOUT` / `XKBVARIANT` lists.
fn from_etc_default_keyboard() -> Option<LayoutNames> {
    let text = std::fs::read_to_string("/etc/default/keyboard").ok()?;
    let value = |name: &str| {
        text.lines()
            .filter_map(|line| line.trim().strip_prefix(name)?.strip_prefix('='))
            .map(|v| {
                v.trim()
                    .trim_matches('"')
                    .split(',')
                    .next()
                    .unwrap_or("")
                    .to_string()
            })
            .next()
    };
    let layout = value("XKBLAYOUT").filter(|l| !l.is_empty())?;
    Some(LayoutNames {
        layout,
        variant: value("XKBVARIANT").unwrap_or_default(),
    })
}

/// Compile `names` and invert it: every character reachable on the first
/// group at levels 0–3 maps to the lowest level (fewest modifiers) that
/// produces it, restricted to keycodes in `allowed` (the uinput device's
/// registered capabilities — an unregistered keycode is silently dropped by
/// the kernel, so it must not be offered).
pub(super) fn build_map(
    names: &LayoutNames,
    allowed: &HashSet<KeyCode>,
) -> Option<HashMap<char, Stroke>> {
    let context = xkb::Context::new(xkb::CONTEXT_NO_FLAGS);
    let keymap = xkb::Keymap::new_from_names(
        &context,
        "",
        "",
        &names.layout,
        &names.variant,
        None,
        xkb::KEYMAP_COMPILE_NO_FLAGS,
    )?;

    let mut map = HashMap::new();
    let (min, max) = (keymap.min_keycode().raw(), keymap.max_keycode().raw());
    for (level, mods) in LEVEL_MODIFIERS.iter().enumerate() {
        let level = u32::try_from(level).ok()?;
        for raw in min..=max {
            let xkb_key = xkb::Keycode::new(raw);
            if level >= keymap.num_levels_for_key(xkb_key, 0) {
                continue;
            }
            let Some(key) = raw
                .checked_sub(XKB_EVDEV_OFFSET)
                .and_then(|code| u16::try_from(code).ok())
                .map(KeyCode::new)
                .filter(|key| allowed.contains(key))
            else {
                continue;
            };
            for sym in keymap.key_get_syms_by_level(xkb_key, 0, level) {
                let Some(ch) = sym.key_char().filter(|c| !c.is_control()) else {
                    continue;
                };
                map.entry(ch).or_insert_with(|| Stroke {
                    mods: mods.to_vec(),
                    key,
                });
            }
        }
    }
    Some(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn every_key() -> HashSet<KeyCode> {
        (1..=0x7F).map(KeyCode::new).collect()
    }

    /// The Spanish layout the fixture machine runs: `@` is AltGr+2 and `-`
    /// sits on the US slash key, exactly the two places a US table gets wrong.
    #[test]
    fn spanish_layout_resolves_symbols_through_altgr() {
        let names = LayoutNames::parse("es");
        let Some(map) = build_map(&names, &every_key()) else {
            eprintln!("xkeyboard-config unavailable; skipping");
            return;
        };
        assert_eq!(
            map[&'@'],
            Stroke {
                mods: vec![KeyCode::KEY_RIGHTALT],
                key: KeyCode::KEY_2
            }
        );
        assert_eq!(
            map[&'a'],
            Stroke {
                mods: vec![],
                key: KeyCode::KEY_A
            }
        );
        assert_eq!(
            map[&'A'],
            Stroke {
                mods: vec![KeyCode::KEY_LEFTSHIFT],
                key: KeyCode::KEY_A
            }
        );
        assert_eq!(map[&'.'].key, KeyCode::KEY_DOT);
        assert_eq!(map[&'-'].key, KeyCode::KEY_SLASH);
        assert_eq!(map[&'ñ'].key, KeyCode::KEY_SEMICOLON);
    }

    #[test]
    fn us_layout_puts_at_sign_on_shift_2() {
        let Some(map) = build_map(&LayoutNames::default(), &every_key()) else {
            eprintln!("xkeyboard-config unavailable; skipping");
            return;
        };
        assert_eq!(
            map[&'@'],
            Stroke {
                mods: vec![KeyCode::KEY_LEFTSHIFT],
                key: KeyCode::KEY_2
            }
        );
    }

    #[test]
    fn unregistered_keycodes_are_left_out() {
        let allowed: HashSet<KeyCode> = [KeyCode::KEY_A].into_iter().collect();
        let Some(map) = build_map(&LayoutNames::default(), &allowed) else {
            return;
        };
        assert!(map.contains_key(&'a'));
        assert!(!map.contains_key(&'b'));
    }

    #[test]
    fn parses_gnome_source_lists() {
        assert_eq!(
            parse_gnome_sources("[('xkb', 'es+cat'), ('xkb', 'us')]"),
            Some(LayoutNames::parse("es+cat"))
        );
        assert_eq!(
            parse_gnome_sources("[('ibus', 'mozc-jp'), ('xkb', 'jp')]"),
            Some(LayoutNames {
                layout: "jp".into(),
                variant: String::new()
            })
        );
        assert_eq!(parse_gnome_sources("@a(ss) []"), None);
    }
}
