"""
Module: checkout_framework.utils.jigging
Description: Address Jigging (防砍單地址變形) 引擎
為避免電商平台（如 Vans / VF Corp、Nike、Shopify）的防欺詐系統（Risk & Fraud Engine）
針對同一個收件地址的多筆訂單進行批量砍單（Order Cancellation），透過演算法對地址進行隨機微調變形。

支援策略：
1. 門牌 / 單元隨機字首字尾 (Random Unit/Suite Padding: Apt #X92, Ste 402, Unit 3B)
2. 道路專有名詞縮寫替換 (Street <-> St, Avenue <-> Ave, Road <-> Rd, Boulevard <-> Blvd)
3. 姓名微調 (Name Jigging: 中間名隨機字母或代號)
4. 電話號碼微調 (Phone Jigging: 隨機尾數擾動)
"""
from __future__ import annotations

import random
import re
import string
from dataclasses import dataclass

# 道路與地址名詞縮寫對應表
STREET_ABBREVIATIONS: dict[str, str] = {
    "street": "St.",
    "st.": "Street",
    "st": "Street",
    "avenue": "Ave.",
    "ave.": "Avenue",
    "ave": "Avenue",
    "boulevard": "Blvd.",
    "blvd.": "Boulevard",
    "blvd": "Boulevard",
    "road": "Rd.",
    "rd.": "Road",
    "rd": "Road",
    "drive": "Dr.",
    "dr.": "Drive",
    "dr": "Drive",
    "lane": "Ln.",
    "ln.": "Lane",
    "ln": "Lane",
    "court": "Ct.",
    "ct.": "Court",
    "ct": "Court",
    "circle": "Cir.",
    "cir.": "Circle",
    "highway": "Hwy.",
    "hwy.": "Highway",
    "parkway": "Pkwy.",
    "pkwy.": "Parkway",
}

UNIT_PREFIXES: list[str] = [
    "Apt",
    "Suite",
    "Unit",
    "Ste",
    "Room",
    "Fl",
    "Floor",
    "Bldg",
    "#",
]


@dataclass
class JigConfig:
    """Address Jigging 配置選項"""
    jig_address_line1: bool = True
    jig_address_line2: bool = True
    jig_name: bool = True
    jig_phone: bool = False
    use_abbreviation_swap: bool = True
    random_unit_min: int = 101
    random_unit_max: int = 999
    noise_length: int = 3       # e.g., 3 位隨機字母數字組合 (X9F)


def generate_noise_tag(length: int = 3) -> str:
    """生成隨機的大寫字母與數字標籤，例如 'A4K', '9RZ'"""
    chars = string.ascii_uppercase + string.digits
    return "".join(random.choices(chars, k=length))


def swap_street_abbreviations(address: str) -> str:
    """替換街道名稱為全稱或縮寫"""
    tokens = address.split(" ")
    new_tokens: list[str] = []
    for token in tokens:
        clean_token = token.lower().strip()
        if clean_token in STREET_ABBREVIATIONS:
            # 依機率替換或保留
            replacement = STREET_ABBREVIATIONS[clean_token]
            new_tokens.append(replacement)
        else:
            new_tokens.append(token)
    return " ".join(new_tokens)


def jig_shipping_address(
    first_name: str,
    last_name: str,
    address1: str,
    address2: str | None = None,
    city: str = "",
    state_code: str = "CA",
    postal_code: str = "92626",
    phone: str = "7145550192",
    config: JigConfig | None = None,
) -> dict[str, str]:
    """
    執行收件人資訊防砍單變形。
    保留城市 (City)、州代碼 (State) 與郵遞區號 (Postal Code) 的真實性，確保物流精確送達；
    對姓名、門牌地址與電話進行特徵混淆。
    """
    cfg = config or JigConfig()
    noise = generate_noise_tag(cfg.noise_length)

    # 1. 姓名變形 (Name Jig)
    jigged_first = first_name
    jigged_last = last_name
    if cfg.jig_name:
        random_initial = random.choice(string.ascii_uppercase)
        # 例如: John -> John X. 或 Doe -> Doe #8K
        jigged_first = f"{first_name} {random_initial}."

    # 2. 地址第一行變形 (Address Line 1 Jig)
    jigged_addr1 = address1
    if cfg.use_abbreviation_swap:
        jigged_addr1 = swap_street_abbreviations(jigged_addr1)

    if cfg.jig_address_line1 and not address2:
        # 若無 address2，在 address1 尾部注入單元號碼
        prefix = random.choice(UNIT_PREFIXES)
        unit_num = random.randint(cfg.random_unit_min, cfg.random_unit_max)
        jigged_addr1 = f"{jigged_addr1} {prefix} {unit_num} - {noise}"

    # 3. 地址第二行變形 (Address Line 2 Jig)
    jigged_addr2 = address2 or ""
    if cfg.jig_address_line2:
        prefix = random.choice(UNIT_PREFIXES)
        unit_num = random.randint(cfg.random_unit_min, cfg.random_unit_max)
        if address2:
            jigged_addr2 = f"{address2} #{noise}"
        else:
            jigged_addr2 = f"{prefix} {unit_num} - {noise}"

    # 4. 電話號碼微調 (Phone Jig)
    jigged_phone = phone
    if cfg.jig_phone:
        digits = re.sub(r"\D", "", phone)
        if len(digits) >= 10:
            # 隨機修改最後 1~2 碼以規避重複偵測 (若不需要簡訊驗證收件)
            prefix_digits = digits[:-2]
            random_end = f"{random.randint(10, 99)}"
            jigged_phone = prefix_digits + random_end

    return {
        "firstName": jigged_first,
        "lastName": jigged_last,
        "address1": jigged_addr1,
        "address2": jigged_addr2,
        "city": city,
        "stateCode": state_code,
        "postalCode": postal_code,
        "countryCode": "US",
        "phone": jigged_phone,
    }
