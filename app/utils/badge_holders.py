"""Polymarket Badge Holder utility module."""

from typing import Optional

# List of X usernames that are Polymarket badge holders
# Normalized without @ prefix for easier matching
BADGE_HOLDERS = {
    "0xdemoo",
    "0x_saurav",
    "0xd1namit",
    "0xdreamwalker",
    "0xgingergirl",
    "0xinfringe",
    "0xinternetchild",
    "0xjim7",
    "0xnyong",
    "0xwondr",
    "22cmdeamorr",
    "25usdc",
    "aaronruanzs",
    "alphaxonp",
    "anselfang",
    "aoq059145049",
    "arcanic",
    "bagcalls",
    "betwick1",
    "brokietrades",
    "csp_trading",
    "cutnpaste4",
    "caronpolymarket",
    "coffeelover_pm",
    "cryptomoonday",
    "cryptokkkai",
    "dancingeddie_",
    "dannyt1502",
    "diditrading",
    "diemondhandz",
    "dropperpm",
    "dyor_0x",
    "euanker",
    "fridayntrades",
    "godrama_",
    "greekgamblerpm",
    "hans323",
    "harveymackinto2",
    "jeonghaeju",
    "kevinzbtc",
    "krasnalwojtek",
    "ljonbtc",
    "leothehorseman",
    "levigmi",
    "lilmoonlambo",
    "mepponpm",
    "maranscrypto",
    "maximilian_evm",
    "mistkygo",
    "momentumkevin",
    "mrkangaroox",
    "mrozipm",
    "mubayy",
    "parallelairev",
    "parzivalpm",
    "phantombets",
    "polyinsider_",
    "poly_noob_",
    "polymarketog",
    "purpman123",
    "remember_amalek",
    "route2fi",
    "satoshiancap",
    "schofield",
    "semioticrivalry",
    "thewolfofpoly",
    "xpredictor",
    "x_playerr",
    "_decap",
    "aadvark89",
    "aaravxbt",
    "abcdefmlzy",
    "aenews",
    "akasonix",
    "akstar82",
    "baeko_02",
    "beefnoodle",
    "bogdikery",
    "buckyandlucky",
    "cashypoly",
    "chainyoda",
    "chandra1",
    "cryptoxiaoxiang",
    "dedsec",
    "default717",
    "dukexbt_",
    "fatfinger",
    "grazkag",
    "hantengri",
    "harrysew",
    "holy_moses7",
    "iforgor_pm",
    "joostienxd",
    "love_u4ever",
    "mango_lassi",
    "mauritsneo",
    "meepie",
    "meustermint",
    "mombil",
    "mrseven_1",
    "nicoco89poly",
    "one8tyfive",
    "penguin_pmkt",
    "player1",
    "polyint",
    "polymarket_o3o",
    "polymarketbet",
    "polymaster",
    "probabilitygod",
    "prophet_notes",
    "r_gopfan",
    "rakshithbv",
    "redstonepm",
    "scottonpoly",
    "shubhh_hum",
    "silverfang88",
    "tomdnc",
    "tsmultra",
    "tsybka",
    "verrissimus",
    "whalewatchpoly",
    "winzon26",
    "xk0neko",
    "yelif_",
}


def normalize_x_username(username: Optional[str]) -> Optional[str]:
    """
    Normalize X username for comparison.
    
    Args:
        username: X username (may include @ prefix)
    
    Returns:
        Normalized username (lowercase, without @ prefix) or None
    """
    if not username:
        return None
    
    # Remove @ prefix if present and convert to lowercase
    normalized = username.strip().lower()
    if normalized.startswith("@"):
        normalized = normalized[1:]
    
    return normalized if normalized else None


def is_badge_holder(x_username: Optional[str]) -> bool:
    """
    Check if an X username is a Polymarket badge holder.
    
    Args:
        x_username: X username to check (may include @ prefix)
    
    Returns:
        True if the username is in the badge holder list, False otherwise
    """
    normalized = normalize_x_username(x_username)
    if not normalized:
        return False
    
    return normalized in BADGE_HOLDERS
