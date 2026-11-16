//! The developer-maintained list of known backbones (`backbones.toml`).

use serde::Deserialize;
use std::sync::LazyLock;
use std::time::Duration;

const BUILTIN: &str = include_str!("backbones.toml");

/// Brands shorter than this (after normalizing) match too much by accident.
const MIN_BRAND_LEN: usize = 5;

#[derive(Deserialize)]
struct RegistryFile {
    probe: ProbeFile,
    backbone: Vec<BackboneFile>,
}

#[derive(Deserialize)]
struct ProbeFile {
    groups: Vec<String>,
    interval_hours: u64,
}

#[derive(Deserialize)]
struct BackboneFile {
    name: String,
    #[serde(default)]
    brands: Vec<String>,
}

/// Known backbones, the groups to compare article numbers in, and the brand
/// hints that can name a group of servers.
#[derive(Debug)]
pub(crate) struct Registry {
    probe_groups: Vec<String>,
    interval: Duration,
    backbones: Vec<String>,
    /// Normalized brand and the one backbone it is listed under.
    hints: Vec<(String, String)>,
}

impl Registry {
    /// The registry compiled into the binary.
    pub(crate) fn builtin() -> &'static Self {
        static REGISTRY: LazyLock<Registry> =
            LazyLock::new(|| Registry::parse(BUILTIN).expect("the built-in backbones.toml parses"));
        &REGISTRY
    }

    pub(crate) fn parse(text: &str) -> Result<Self, toml::de::Error> {
        let file: RegistryFile = toml::from_str(text)?;
        let backbones = file
            .backbone
            .iter()
            .map(|backbone| backbone.name.trim().to_ascii_lowercase())
            .collect();
        let mut listed: Vec<(String, String)> = Vec::new();
        for backbone in &file.backbone {
            for brand in &backbone.brands {
                let brand = normalize(brand);
                if brand.len() >= MIN_BRAND_LEN {
                    listed.push((brand, backbone.name.trim().to_ascii_lowercase()));
                }
            }
        }
        // A brand under several backbones is a multi-backbone reseller: it
        // says nothing about which one a given server of theirs is on.
        let hints = listed
            .iter()
            .filter(|(brand, backbone)| {
                listed
                    .iter()
                    .all(|(other, other_backbone)| other != brand || other_backbone == backbone)
            })
            .cloned()
            .collect();
        Ok(Self {
            probe_groups: file.probe.groups,
            interval: Duration::from_secs(file.probe.interval_hours.max(1) * 3600),
            backbones,
            hints,
        })
    }

    pub(crate) fn probe_groups(&self) -> &[String] {
        &self.probe_groups
    }

    pub(crate) const fn interval(&self) -> Duration {
        self.interval
    }

    pub(crate) fn backbone_names(&self) -> &[String] {
        &self.backbones
    }

    /// The one backbone whose brands appear in `texts`, if exactly one does.
    pub(crate) fn hint<'a>(&self, texts: impl IntoIterator<Item = &'a str>) -> Option<&str> {
        let texts: Vec<String> = texts.into_iter().map(normalize).collect();
        let mut found: Option<&str> = None;
        for (brand, backbone) in &self.hints {
            if texts.iter().any(|text| text.contains(brand.as_str())) {
                match found {
                    None => found = Some(backbone),
                    Some(previous) if previous == backbone => {}
                    Some(_) => return None,
                }
            }
        }
        found
    }
}

/// Lowercase letters and digits only, so "XS News", "xsnews.nl" and
/// "News.XSNews.com" can be compared.
fn normalize(text: &str) -> String {
    text.chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_builtin_registry_parses_and_lists_probe_groups() {
        let registry = Registry::builtin();
        assert!(!registry.probe_groups().is_empty());
        assert!(
            registry
                .backbone_names()
                .iter()
                .any(|name| name == "omicron")
        );
        assert!(registry.interval() >= Duration::from_secs(3600));
    }

    #[test]
    fn a_brand_in_a_hostname_or_greeting_names_its_backbone() {
        let registry = Registry::builtin();
        assert_eq!(registry.hint(["news.xsnews.nl"]), Some("abavia"));
        assert_eq!(
            registry.hint(["super.example.com", "200 News.GigaNews.Com"]),
            Some("giganews")
        );
        assert_eq!(registry.hint(["news.example.org", "200 Welcome"]), None);
    }

    #[test]
    fn a_reseller_of_several_backbones_is_never_a_hint() {
        let registry = Registry::parse(
            r#"
            [probe]
            groups = ["alt.test"]
            interval_hours = 6
            [[backbone]]
            name = "one"
            brands = ["SharedBrand", "OnlyOne"]
            [[backbone]]
            name = "two"
            brands = ["SharedBrand"]
            "#,
        )
        .unwrap();
        assert_eq!(registry.hint(["news.sharedbrand.example"]), None);
        assert_eq!(registry.hint(["news.onlyone.example"]), Some("one"));
        assert_eq!(
            registry.hint(["news.onlyone.example", "200 SharedBrand"]),
            Some("one"),
            "an ambiguous brand next to a clear one leaves the clear one"
        );
    }

    #[test]
    fn texts_naming_two_backbones_give_no_hint() {
        let registry = Registry::parse(
            r#"
            [probe]
            groups = ["alt.test"]
            interval_hours = 6
            [[backbone]]
            name = "one"
            brands = ["BrandOne"]
            [[backbone]]
            name = "two"
            brands = ["BrandTwo"]
            "#,
        )
        .unwrap();
        assert_eq!(registry.hint(["brandone.example", "200 BrandTwo"]), None);
    }
}
