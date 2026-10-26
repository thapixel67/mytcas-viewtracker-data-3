#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
myTCAS ViewTracker - Nationwide Course Scraper & Data Pipeline (Fixed Parser)
"""

import asyncio
import json
import logging
import os
import random
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Any

import aiohttp
from bs4 import BeautifulSoup

TZ_TH = timezone(timedelta(hours=7))

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("myTCAS-Scraper")
logger.setLevel(logging.INFO)
logger.addHandler(handler)
logger.propagate = False

BASE_URL = "https://course.mytcas.com"
MAX_CONCURRENT_REQUESTS = 30
REQUEST_TIMEOUT_SEC = 18
SOCK_READ_TIMEOUT = 12
MAX_RETRIES = 3
OUTPUT_FILE = Path("data.json")
SPLIT_DIR = Path("data/universities")

CANDIDATE_JSON_ENDPOINTS = [
    "https://my-tcas.s3.ap-southeast-1.amazonaws.com/mytcas/courses.json",
    "https://my-tcas.s3.ap-southeast-1.amazonaws.com/mytcas/programs.json",
    "https://my-tcas.s3.ap-southeast-1.amazonaws.com/mytcas/universities.json",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
]

VIEW_COUNT_PATTERNS = [
    re.compile(r"ยอดผู้เข้าชม\s*[:\-]?\s*([0-9,]+)\s*ครั้ง", re.IGNORECASE),
    re.compile(r"ผู้เข้าชม\s*[:\-]?\s*([0-9,]+)\s*ครั้ง", re.IGNORECASE),
    re.compile(r'"(?:views|view_count|viewCount|viewsCount|hits|pageviews|total_views)"\s*:\s*([0-9]+)', re.IGNORECASE),
]

def get_headers() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json,*/*;q=0.8",
        "Accept-Language": "th-TH,th;q=0.9,en-US;q=0.8",
        "Referer": "https://course.mytcas.com/",
        "Origin": "https://course.mytcas.com",
        "X-Forwarded-For": f"171.96.{random.randint(10, 250)}.{random.randint(10, 250)}",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
    }

def classify_faculty_group(faculty: str, program_name: str) -> str:
    text = f"{faculty} {program_name}".lower()
    if any(k in text for k in ["แพทยศาสตร", "ทันตแพทย", "เภสัชศาสตร", "สัตวแพทย"]):
        return "แพทย์ / ทันตะ / เภสัช / สัตวแพทย์"
    if any(k in text for k in ["พยาบาล", "สหเวช", "เทคนิคการแพทย์", "สาธารณสุข", "กายภาพบำบัด"]):
        return "พยาบาล / สหเวช / สุขภาพ"
    if any(k in text for k in ["คอมพิวเตอร์", "ดิจิทัล", "สารสนเทศ", "ปัญญาประดิษฐ์", "ข้อมูล", "ไอที", "ai", "data", "cyber"]):
        return "วิทยาการคอม / IT / ดิจิทัล"
    if any(k in text for k in ["วิศวกรรม", "ช่างอุตสาหกรรม"]):
        return "วิศวกรรมศาสตร์"
    if any(k in text for k in ["บริหารธุรกิจ", "บัญชี", "พาณิชยศาสตร์", "การตลาด", "การจัดการ", "โลจิสติกส์", "การเงิน"]):
        return "บริหารธุรกิจ / บัญชี"
    if "เศรษฐศาสตร" in text:
        return "เศรษฐศาสตร์"
    if any(k in text for k in ["นิติศาสตร", "กฎหมาย", "รัฐศาสตร", "การปกครอง"]):
        return "นิติศาสตร์ / รัฐศาสตร์"
    if any(k in text for k in ["อักษรศาสตร", "ศิลปศาสตร", "มนุษยศาสตร", "ภาษา"]):
        return "มนุษยศาสตร์ / ศิลปศาสตร์ / ภาษา"
    if any(k in text for k in ["ครุศาสตร", "ศึกษาศาสตร", "ครู"]):
        return "ครุศาสตร์ / ศึกษาศาสตร์"
    if any(k in text for k in ["สถาปัตยกรรม", "ศิลปกรรม", "นิเทศศาสตร", "การออกแบบ"]):
        return "สถาปัตย์ / ศิลปกรรม / นิเทศศาสตร์"
    if any(k in text for k in ["เกษตร", "ประมง", "วนศาสตร", "อุตสาหกรรมเกษตร"]):
        return "เกษตรศาสตร์ / อุตสาหกรรมเกษตร"
    if "วิทยาศาสตร" in text:
        return "วิทยาศาสตร์"
    return "อื่นๆ / สหวิทยาการ"

def extract_val(d: Dict, *keys: str) -> str:
    for k in keys:
        v = d.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""

async def fetch_with_retry(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore, expect_json: bool = False) -> Optional[Any]:
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SEC, sock_read=SOCK_READ_TIMEOUT)
    for attempt in range(1, MAX_RETRIES + 1):
        async with semaphore:
            await asyncio.sleep(random.uniform(0.03, 0.08))
            try:
                async with session.get(url, headers=get_headers(), timeout=timeout) as response:
                    if response.status == 200:
                        if expect_json:
                            return await response.json(content_type=None)
                        return await response.text(errors="replace")
                    if response.status in (429, 502, 503, 504):
                        await asyncio.sleep(1.2 * attempt)
                        continue
                    if response.status == 404:
                        return None
            except Exception:
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(0.5 * attempt)
    return None

class MasterCatalogDiscovery:
    def __init__(self, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore):
        self.session = session
        self.semaphore = semaphore
        self.universities: Dict[str, str] = {}
        self.programs_catalog: Dict[str, Dict] = {}

    async def run_discovery(self) -> Dict[str, Dict]:
        logger.info("🔍 [Stage 1] เริ่มค้นหาฐานข้อมูลหลักสูตรทั้งหมดทั่วประเทศ...")

        # ดึงรายชื่อมหาวิทยาลัย
        uni_data = await fetch_with_retry(self.session, "https://my-tcas.s3.ap-southeast-1.amazonaws.com/mytcas/universities.json", self.semaphore, expect_json=True)
        if isinstance(uni_data, list):
            for item in uni_data:
                if isinstance(item, dict):
                    uid = extract_val(item, "u_id", "university_id", "id")
                    uname = extract_val(item, "u_name", "university_name", "name_th", "name")
                    if uid and uname:
                        self.universities[uid] = uname

        # ดึงรายชื่อหลักสูตรทั้งหมด
        course_data = await fetch_with_retry(self.session, "https://my-tcas.s3.ap-southeast-1.amazonaws.com/mytcas/courses.json", self.semaphore, expect_json=True)
        if isinstance(course_data, list):
            for item in course_data:
                if not isinstance(item, dict):
                    continue
                pid = extract_val(item, "program_id", "id", "code")
                if len(pid) >= 8:
                    self.programs_catalog[pid] = item
                    # บันทึกมหาวิทยาลัยเพิ่มเติม
                    uid = extract_val(item, "u_id", "university_id")
                    uname = extract_val(item, "u_name", "university_name")
                    if uid and uname and uid not in self.universities:
                        self.universities[uid] = uname

        logger.info(f"✅ โหลดฐานข้อมูลสำเร็จ: มหาวิทยาลัย {len(self.universities)} แห่ง | หลักสูตร {len(self.programs_catalog):,} สาขา")
        return self.programs_catalog

class ProgramParser:
    @staticmethod
    def parse(program_id: str, html: Optional[str], raw_meta: Dict, universities_map: Dict[str, str]) -> Dict[str, Any]:
        # 1. สกัดข้อมูลจาก Catalog Meta ก่อนเป็นหลัก
        u_id = extract_val(raw_meta, "u_id", "university_id") or program_id[:3]
        university = extract_val(raw_meta, "u_name", "university_name", "university") or universities_map.get(u_id, "ไม่ระบุสถาบัน")
        faculty = extract_val(raw_meta, "faculty_name", "faculty_name_th", "faculty", "fac_name", "faculty_title") or "ไม่ระบุคณะ"
        
        program_name = extract_val(
            raw_meta, 
            "program_name", "program_name_th", "name_th", "name", 
            "major_name", "major_name_th", "field_name", "title"
        )

        program_type = extract_val(raw_meta, "program_type", "program_type_name_th", "type") or "ภาษาไทย ปกติ"
        campus = extract_val(raw_meta, "campus_name", "campus", "campus_name_th") or "วิทยาเขตหลัก"

        # จำนวนรับแต่ละรอบ
        def get_seat(key: str) -> int:
            val = raw_meta.get(key, 0)
            try:
                return int(str(val).replace(",", "").strip() or 0)
            except ValueError:
                return 0

        r1 = get_seat("r1") or get_seat("round1")
        r2 = get_seat("r2") or get_seat("round2")
        r3 = get_seat("r3") or get_seat("round3")
        r4 = get_seat("r4") or get_seat("round4")

        views = 0

        # 2. หากมี HTML ตอบกลับมา ให้แกะข้อมูลและยอดวิวจาก __NEXT_DATA__
        if html:
            # ค้นหายอดวิวด้วย Regex ก่อน
            for pattern in VIEW_COUNT_PATTERNS:
                m = pattern.search(html)
                if m:
                    raw_views = m.group(1).replace(",", "").strip()
                    if raw_views.isdigit() and int(raw_views) > 0:
                        views = int(raw_views)
                        break

            # แกะ JSON จาก __NEXT_DATA__
            soup = BeautifulSoup(html, "lxml")
            next_tag = soup.find("script", id="__NEXT_DATA__")
            if next_tag and next_tag.string:
                try:
                    payload = json.loads(next_tag.string)
                    raw_str = json.dumps(payload, ensure_ascii=False)
                    # ดึงยอดวิวจาก JSON ภายใน
                    for p_view in VIEW_COUNT_PATTERNS:
                        m_json = p_view.search(raw_str)
                        if m_json and views == 0:
                            v = int(m_json.group(1).replace(",", ""))
                            if v > 0:
                                views = v

                    # เสริมข้อมูลชื่อ/คณะหาก Catalog ขาดหาย
                    page_props = payload.get("props", {}).get("pageProps", {})
                    p_obj = page_props.get("program") or page_props.get("data") or {}
                    if isinstance(p_obj, dict):
                        if not program_name or "ระบบการคัดเลือก" in program_name:
                            program_name = extract_val(p_obj, "program_name", "name_th", "name", "title")
                        if faculty == "ไม่ระบุคณะ":
                            faculty = extract_val(p_obj, "faculty_name", "faculty", "faculty_name_th")
                        if university == "ไม่ระบุสถาบัน":
                            university = extract_val(p_obj, "university_name", "u_name", "university")
                        if r1 == 0 and r2 == 0 and r3 == 0:
                            r1 = int(p_obj.get("r1") or 0)
                            r2 = int(p_obj.get("r2") or 0)
                            r3 = int(p_obj.get("r3") or 0)
                            r4 = int(p_obj.get("r4") or 0)
                except Exception:
                    pass

        # ป้องกันไม่ให้ชื่อกลายเป็นหัวข้อเว็บหลักเด็ดขาด
        if not program_name or "ระบบการคัดเลือก" in program_name:
            field_fallback = extract_val(raw_meta, "field_name", "major_name")
            program_name = field_fallback if field_fallback else f"สาขาวิชา ({program_id})"

        total_seats = r1 + r2 + r3 + r4
        if views == 0:
            meta_views = extract_val(raw_meta, "views", "view_count", "hits")
            if meta_views.isdigit():
                views = int(meta_views)

        return {
            "program_id": program_id,
            "program_name": program_name,
            "faculty": faculty,
            "university": university,
            "university_id": u_id,
            "campus": campus,
            "program_type": program_type,
            "category": classify_faculty_group(faculty, program_name),
            "r1": r1,
            "r2": r2,
            "r3": r3,
            "r4": r4,
            "total_seats": total_seats,
            "views": views,
            "url": f"{BASE_URL}/programs/{program_id}",
        }

async def scrape_all_programs(session: aiohttp.ClientSession, catalog: Dict[str, Dict], universities_map: Dict[str, str]) -> List[Dict[str, Any]]:
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    program_ids = list(catalog.keys())
    total = len(program_ids)
    results: List[Dict[str, Any]] = []

    logger.info(f"🚀 [Stage 2] เริ่มประมวลผลข้อมูลและยอดวิว {total:,} หลักสูตร...")
    start_ts = time.time()
    batch_size = 350

    for start_idx in range(0, total, batch_size):
        batch_ids = program_ids[start_idx : start_idx + batch_size]

        async def process_item(pid: str):
            url = f"{BASE_URL}/programs/{pid}"
            html = await fetch_with_retry(session, url, semaphore)
            return ProgramParser.parse(pid, html, catalog.get(pid, {}), universities_map)

        batch_results = await asyncio.gather(*(process_item(pid) for pid in batch_ids))
        for res in batch_results:
            if res:
                results.append(res)

        done = min(start_idx + batch_size, total)
        elapsed = time.time() - start_ts
        speed = done / elapsed if elapsed > 0 else 0
        logger.info(f"ความคืบหน้า: {done:,}/{total:,} ({done/total*100:.1f}%) | ประมวลผลได้: {len(results):,} สาขา ({speed:.1f} สาขา/วินาที)")

    return results

def compile_and_save(programs: List[Dict[str, Any]]) -> None:
    logger.info("📊 [Stage 3] กำลังจัดอันดับและบันทึกไฟล์ data.json...")

    prev_map = {}
    if OUTPUT_FILE.exists():
        try:
            with OUTPUT_FILE.open("r", encoding="utf-8") as f:
                old_payload = json.load(f)
            for row in old_payload.get("programs", []):
                if row.get("program_id"):
                    prev_map[row["program_id"]] = (int(row.get("views", 0)), int(row.get("rank", 0)))
        except Exception:
            pass

    # เรียงลำดับตามยอดวิว
    programs.sort(key=lambda x: (x["views"], x["total_seats"]), reverse=True)

    total_views = 0
    total_seats = 0
    by_university = {}

    for idx, item in enumerate(programs, start=1):
        item["rank"] = idx
        pid = item["program_id"]
        prev_views, _ = prev_map.get(pid, (item["views"], idx))
        item["views_prev"] = prev_views
        item["views_diff"] = max(0, item["views"] - prev_views)

        total_views += item["views"]
        total_seats += item["total_seats"]
        by_university.setdefault(item.get("university_id") or "other", []).append(item)

    now_th = datetime.now(TZ_TH)
    payload = {
        "metadata": {
            "updated_at": now_th.strftime("%Y-%m-%d %H:%M:%S"),
            "cycle": "รอบเช้า (06:00 น.)" if now_th.hour < 12 else "รอบเย็น (18:00 น.)",
            "total_programs": len(programs),
            "total_universities": len({p["university"] for p in programs if p["university"] != "ไม่ระบุสถาบัน"}),
            "total_views": total_views,
            "total_seats": total_seats,
        },
        "programs": programs,
    }

    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    SPLIT_DIR.mkdir(parents=True, exist_ok=True)
    for u_id, uni_progs in by_university.items():
        safe_id = re.sub(r"[^0-9A-Za-z_\-]", "_", u_id)
        with (SPLIT_DIR / f"{safe_id}.json").open("w", encoding="utf-8") as f:
            json.dump(uni_progs, f, ensure_ascii=False, separators=(",", ":"))

    logger.info(f"🎉 สำเร็จ! บันทึกข้อมูล {len(programs):,} สาขาลง data.json เรียบร้อยแล้ว")

async def main() -> None:
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS, ttl_dns_cache=300, ssl=False)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(connector=connector) as session:
        discovery = MasterCatalogDiscovery(session, semaphore)
        catalog = await discovery.run_discovery()

        if not catalog:
            logger.error("❌ ไม่พบข้อมูลหลักสูตร")
            return

        programs = await scrape_all_programs(session, catalog, discovery.universities)
        if programs:
            compile_and_save(programs)

if __name__ == "__main__":
    asyncio.run(main())
