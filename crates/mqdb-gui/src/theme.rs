use eframe::egui::{self, Color32, CornerRadius, Stroke, Style, Visuals, epaint::Shadow};

const BG_DARK: Color32 = Color32::from_rgb(18, 18, 24);
const BG_PANEL: Color32 = Color32::from_rgb(24, 24, 32);
const BG_WIDGET: Color32 = Color32::from_rgb(32, 34, 44);
const BG_WIDGET_HOVER: Color32 = Color32::from_rgb(42, 44, 56);
const BG_WIDGET_ACTIVE: Color32 = Color32::from_rgb(50, 52, 68);
const BG_EXTREME: Color32 = Color32::from_rgb(14, 14, 18);

const TEXT_PRIMARY: Color32 = Color32::from_rgb(220, 222, 230);
const TEXT_DIM: Color32 = Color32::from_rgb(140, 144, 160);

const ACCENT: Color32 = Color32::from_rgb(0, 188, 180);
const ACCENT_HOVER: Color32 = Color32::from_rgb(30, 210, 200);
const ACCENT_DIM: Color32 = Color32::from_rgb(0, 140, 134);

pub const SUCCESS: Color32 = Color32::from_rgb(80, 200, 120);
pub const ERROR: Color32 = Color32::from_rgb(230, 80, 80);
pub const WARNING: Color32 = Color32::from_rgb(230, 180, 60);
pub const CREATE_COLOR: Color32 = Color32::from_rgb(80, 200, 120);
pub const UPDATE_COLOR: Color32 = Color32::from_rgb(100, 160, 230);
pub const DELETE_COLOR: Color32 = Color32::from_rgb(230, 80, 80);

const SEPARATOR: Color32 = Color32::from_rgb(44, 46, 58);

pub fn apply(ctx: &egui::Context) {
    let mut style = Style::default();

    style.spacing.item_spacing = egui::vec2(8.0, 6.0);
    style.spacing.window_margin = egui::Margin::same(12);
    style.spacing.button_padding = egui::vec2(10.0, 4.0);
    style.spacing.interact_size = egui::vec2(40.0, 22.0);
    style.spacing.indent = 18.0;

    let corner = CornerRadius::same(4);

    let mut visuals = Visuals::dark();

    visuals.override_text_color = Some(TEXT_PRIMARY);
    visuals.panel_fill = BG_PANEL;
    visuals.window_fill = BG_PANEL;
    visuals.extreme_bg_color = BG_EXTREME;
    visuals.faint_bg_color = Color32::from_rgb(28, 30, 40);

    visuals.selection.bg_fill = ACCENT_DIM;
    visuals.selection.stroke = Stroke::new(1.0, ACCENT);

    visuals.hyperlink_color = ACCENT;

    visuals.window_shadow = Shadow {
        offset: [0, 4],
        blur: 12,
        spread: 0,
        color: Color32::from_black_alpha(80),
    };
    visuals.window_corner_radius = CornerRadius::same(6);
    visuals.window_stroke = Stroke::new(1.0, SEPARATOR);

    visuals.widgets.noninteractive.bg_fill = BG_WIDGET;
    visuals.widgets.noninteractive.fg_stroke = Stroke::new(1.0, TEXT_DIM);
    visuals.widgets.noninteractive.bg_stroke = Stroke::new(0.5, SEPARATOR);
    visuals.widgets.noninteractive.corner_radius = corner;

    visuals.widgets.inactive.bg_fill = BG_WIDGET;
    visuals.widgets.inactive.fg_stroke = Stroke::new(1.0, TEXT_PRIMARY);
    visuals.widgets.inactive.bg_stroke = Stroke::new(0.5, SEPARATOR);
    visuals.widgets.inactive.corner_radius = corner;

    visuals.widgets.hovered.bg_fill = BG_WIDGET_HOVER;
    visuals.widgets.hovered.fg_stroke = Stroke::new(1.0, Color32::WHITE);
    visuals.widgets.hovered.bg_stroke = Stroke::new(1.0, ACCENT);
    visuals.widgets.hovered.corner_radius = corner;

    visuals.widgets.active.bg_fill = BG_WIDGET_ACTIVE;
    visuals.widgets.active.fg_stroke = Stroke::new(1.0, ACCENT);
    visuals.widgets.active.bg_stroke = Stroke::new(1.0, ACCENT_HOVER);
    visuals.widgets.active.corner_radius = corner;

    visuals.widgets.open.bg_fill = BG_WIDGET_ACTIVE;
    visuals.widgets.open.fg_stroke = Stroke::new(1.0, ACCENT);
    visuals.widgets.open.bg_stroke = Stroke::new(1.0, ACCENT);
    visuals.widgets.open.corner_radius = corner;

    visuals.striped = true;
    visuals.slider_trailing_fill = true;

    style.visuals = visuals;
    ctx.set_style(style);
}

pub fn bg_dark() -> Color32 {
    BG_DARK
}

pub fn accent() -> Color32 {
    ACCENT
}

pub fn text_dim() -> Color32 {
    TEXT_DIM
}

pub fn separator() -> Color32 {
    SEPARATOR
}
