//! 256 short, distinct words for human-friendly codes (8 bits each).

pub const WORDS: [&str; 256] = [
    "acid", "acorn", "actor", "adobe", "agent", "alarm", "album", "alpha", "amber", "anchor",
    "angle", "apple", "april", "apron", "arch", "arena", "arrow", "atlas", "atom", "aunt",
    "autumn", "avenue", "badge", "bagel", "baker", "bamboo", "banjo", "barn", "basil", "beach",
    "beacon", "bear", "beetle", "bell", "berry", "bicycle", "bison", "blade", "blanket", "blossom",
    "boat", "bonsai", "boot", "bottle", "bracket", "brick", "bridge", "bronze", "brook", "bucket",
    "buffalo", "butter", "cabin", "cactus", "camel", "camera", "candle", "canoe", "canyon",
    "carbon", "cargo", "carrot", "castle", "cedar", "cello", "chalk", "cherry", "chess", "chimney",
    "cider", "circus", "citrus", "clam", "cliff", "clock", "cloud", "clover", "cobalt", "cocoa",
    "comet", "copper", "coral", "cotton", "cougar", "crane", "crater", "cricket", "crystal", "cup",
    "cyclone", "daisy", "delta", "denim", "desert", "diamond", "dingo", "dolphin", "domino",
    "donkey", "dragon", "drum", "dune", "eagle", "earth", "easel", "echo", "eclipse", "elbow",
    "elder", "ember", "emerald", "engine", "falcon", "fan", "feather", "fern", "ferry", "fiddle",
    "fig", "flame", "flask", "flint", "flute", "forest", "fossil", "fox", "frost", "galaxy",
    "garden", "garlic", "gecko", "geyser", "ginger", "glacier", "globe", "goat", "gold", "gopher",
    "granite", "grape", "gravel", "guitar", "hammer", "harbor", "harp", "hazel", "helmet", "heron",
    "hickory", "honey", "horizon", "hornet", "husky", "igloo", "indigo", "iris", "island", "ivory",
    "jacket", "jaguar", "jasmine", "jelly", "jigsaw", "juniper", "kayak", "kettle", "kiwi",
    "koala", "lagoon", "lantern", "lava", "lemon", "lentil", "lilac", "linen", "lizard", "lobster",
    "lotus", "lunar", "magnet", "mango", "maple", "marble", "meadow", "melon", "meteor", "mint",
    "mirror", "mocha", "monsoon", "moose", "mosaic", "nectar", "needle", "nickel", "noodle",
    "nova", "nutmeg", "oasis", "ocean", "olive", "onyx", "orbit", "orchid", "otter", "owl",
    "oyster", "paddle", "panda", "papaya", "parrot", "peach", "pebble", "pelican", "pepper",
    "piano", "pigeon", "pine", "pixel", "planet", "plum", "polar", "pony", "poppy", "prairie",
    "prism", "pumpkin", "quartz", "quill", "rabbit", "radar", "raven", "reef", "ribbon", "river",
    "robin", "rocket", "saffron", "salmon", "sapphire", "saturn", "shell", "silver", "sparrow",
    "spruce", "summit", "tiger", "tulip", "violet", "walnut", "willow", "zebra", "yacht", "yarrow",
    "zenith", "zinc",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn words_are_unique_and_lowercase() {
        let set: std::collections::HashSet<_> = WORDS.iter().collect();
        assert_eq!(set.len(), WORDS.len());
        assert!(
            WORDS
                .iter()
                .all(|w| w.chars().all(|c| c.is_ascii_lowercase()))
        );
    }
}
