use rmk::types::action::{Action, KeyAction};
use rmk::types::keycode::{KeyCode, HidKeyCode};
use rmk::{a, k, layer, mo};
use rmk::types::modifier::ModifierCombination;
pub(crate) const COL: usize = 6;
pub(crate) const ROW: usize = 5;
pub(crate) const NUM_LAYER: usize = 2;

pub const fn shifted(key: HidKeyCode) -> KeyAction {
    KeyAction::Single(Action::KeyWithModifier(KeyCode::Hid(key), ModifierCombination::LSHIFT))
}

#[rustfmt::skip]
pub const fn get_default_keymap() -> [[[KeyAction; COL]; ROW]; NUM_LAYER] {
    [
        // ==========================================
        // LAYER 0: Base Layer (Left of the '/')
        // ==========================================
        layer!([
            // Row 0: 6 7 8 9 0 -
            [k!(Kc6), k!(Kc7), k!(Kc8), k!(Kc9), k!(Kc0), k!(Minus)],
            // Row 1: Y U I O P =
            [k!(Y),   k!(U),   k!(I),   k!(O),   k!(P),   k!(Equal)],
            // Row 2: H J K L ; "
            [k!(H),   k!(J),   k!(K),   k!(L),   k!(Semicolon), shifted(HidKeyCode::Kc2)],
            // Row 3: N M , . / fn
            [k!(N),   k!(M),   k!(Comma), k!(Dot), k!(Slash), mo!(1)],
            // Row 4: Thumbs (Space -> Enter -> Backspace) + 3 mock values
            [k!(Space), k!(Enter), k!(Backspace), a!(No), a!(No), a!(No)]
        ]),

        // ==========================================
        // LAYER 1: Symbols & Arrows (Right of the '/')
        // Activated by holding the bottom-right key
        // ==========================================
        layer!([
            // Row 0: F6 F7 F8 F9 F10 F11
            [k!(F6),  k!(F7),  k!(F8),  k!(F9),  k!(F10), k!(F11)],
            // Row 1: { } [ ] ( )
            [shifted(HidKeyCode::LeftBracket), shifted(HidKeyCode::RightBracket), k!(LeftBracket), k!(RightBracket), shifted(HidKeyCode::Kc9), shifted(HidKeyCode::Kc0)],
            // Row 2: Left Up Down Right (Arrows)
            [k!(Left), k!(Up), k!(Down), k!(Right), a!(No), a!(No)],
            // Row 3: ! $ % ^ ~ -> Mapped to base keys (1, 4, 5, 6, Grave)
            [k!(Kc1), k!(Kc4), k!(Kc5), k!(Kc6), k!(Grave), a!(No)],
            // Row 4: Keep Thumbs functional while holding the layer toggle
            [k!(Space), k!(Enter), k!(Backspace), a!(No), a!(No), a!(No)]
        ]),
    ]
}
