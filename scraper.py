#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
myTCAS ViewTracker - Nationwide Course Scraper & Data Pipeline (Fast & Non-blocking)
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
from typing import Dict, List, Optional, Set, Tuple, Any

import aiohttp
from bs4 import BeautifulSoup

# Timezone ไทย (UTC+7)
TZ_TH = timezone(timedelta(hours=7))

# ตั้งค่า Logging ให้ Flush ออกทันที ไม่เก็บค้างในบัฟเฟอร์
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("myTCAS-Scraper")
logger.setLevel(logging.INFO)
logger.addHandler(handler)
logger.propagate = False

BASE_URL = "https://course.mytcas.com"
UNIVERSITIES_URL = f"{BASE_URL}/universities"

CANDIDATE_JSON_ENDPOINTS = [
    "https://my-tcas.s3.ap-southeast-1.amazonaws.com/mytcas/courses.json",
    "https://my-tcas.s3.ap-southeast-1.amazonaws.com/mytcas/programs.json",
    "https://my-tcas.s3.ap-southeast-1.amazonaws.com/mytcas/universities.json",
]

MAX_CONCURRENT_REQUESTS = 35     # จำนวนคำขอพร้อมกัน
REQUEST_TIMEOUT_SEC = 16         # Timeout รวมต่อหน้า
SOCK_READ_TIMEOUT = 10           # ป้องกันการค้าง Socket
MAX_RETRIES = 3                  # ลองใหม่สูงสุด 3 ครั้ง
OUTPUT_FILE = Path("data.json")
SPLIT_DIR = Path("data/universities")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
]

VIEW_COUNT_PATTERNS = [
    re.compile(r"ยอดผู้เข้าชม\s*[:\-]?\s*([0-9,]+)\s*ครั้ง", re.IGNORECASE),
    re.compile(r"ผู้เข้าชม\s*[:\-]?\s*([0-9,]+)\s*ครั้ง", re.IGNORECASE),
    re.compile(r'"(?:views|view_count|viewCount|hits)"\s*:\s*([0-9]+)', re.IGNORECASE),
]

def get_headers() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json,*/*;q=0.8",
        "Accept-Language": "th-TH,th;q=0.9,en-US;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Referer": BASE_URL,
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
    }

def classify_faculty_group(faculty: str, program_name: str) -> str:
    text = f"{faculty} {program_name}".lower()
    if any(k in text for k in ["แพทยศาสตร", "ทันตแพทย", "เภสัชศาสตร", "สัตวแพทย"]):
        return "แพทย์ / ทันตะ / เภสัช / สัตวแพทย์"
    if any(k in text for k in ["พยาบาล", "สหเวช", "เทคนิคการแพทย์", "สาธารณสุข", "กายภาพบำบัด"]):
        return "พยาบาล / สหเวช / สุขภาพ"
    if any(k in text for k in ["คอมพิวเตอร์", "ดิจิทัล", "สารสนเทศ", "ปัญญาประดิษฐ์", "ข้อมูล", "ไอที", "ai", "data"]):
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

async def fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
    expect_json: bool = False,
) -> Optional[Any]:
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SEC, sock_read=SOCK_READ_TIMEOUT)
    for attempt in range(1, MAX_RETRIES + 1):
        async with semaphore:
            await asyncio.sleep(random.uniform(0.02, 0.08))
            try:
                async with session.get(url, headers=get_headers(), timeout=timeout) as response:
                    if response.status == 200:
                        if expect_json:
                            return await response.json(content_type=None)
                        return await response.text(errors="replace")
                    if response.status in (429, 502, 503, 504):
                        await asyncio.sleep((1.5 ** attempt) + random.uniform(0.1, 0.5))
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
        self.program_ids: Set[str] = set()
        self.prefetched_meta: Dict[str, Dict] = {}

    async def run_discovery(self) -> List[str]:
        logger.info("🔍 [Stage 1] เริ่มค้นหารายชื่อหลักสูตรทั้งหมดทั่วประเทศ...")
        for endpoint in CANDIDATE_JSON_ENDPOINTS:
            data = await fetch_with_retry(self.session, endpoint, self.semaphore, expect_json=True)
            if not data or not isinstance(data, list):
                continue

            for item in data:
                if not isinstance(item, dict):
                    continue
                u_id = str(item.get("u_id") or item.get("university_id") or item.get("id") or "").strip()
                u_name = str(item.get("u_name") or item.get("university_name") or item.get("name_th") or "").strip()
                if u_id and u_name and len(u_id) <= 6:
                    self.universities[u_id] = u_name

                p_id = str(item.get("program_id") or item.get("id") or "").strip()
                if len(p_id) >= 8:
                    self.program_ids.add(p_id)
                    self.prefetched_meta[p_id] = item

            logger.info(f"✅ โหลดฐานข้อมูลจาก {endpoint}: มหาวิทยาลัย {len(self.universities)} แห่ง, หลักสูตร {len(self.program_ids)} สาขา")

        # หากได้หลักสูตรจาก S3 แล้ว ให้ข้ามการ Web Crawl หน้าเว็บ 92 แห่งทันทีเพื่อความเร็ว
        if len(self.program_ids) >= 1000:
            logger.info(f"⚡ พบหลักสูตรจากฐานข้อมูลแล้ว {len(self.program_ids):,} สาขา (ข้ามขั้นตอน Web Crawl)")
            return sorted(self.program_ids)

        # สำรองกรณีดึง S3 ไม่ได้
        html = await fetch_with_retry(self.session, UNIVERSITIES_URL, self.semaphore)
        if html:
            for m in re.finditer(r"/programs/([0-9A-Za-z_\-]{8,25})", html):
                self.program_ids.add(m.group(1))

        return sorted(self.program_ids)

class ProgramParser:
    @staticmethod
    def parse_program_html(
        program_id: str,
        html: str,
        fallback_meta: Optional[Dict] = None,
        universities_map: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        soup = BeautifulSoup(html, "lxml")
        full_text = soup.get_text(" ", strip=True)

        if "ไม่พบข้อมูล" in full_text and len(full_text) < 400:
            return None

        meta = fallback_meta or {}
        university = str(meta.get("u_name") or meta.get("university_name") or "").strip()
        university_id = str(meta.get("u_id") or meta.get("university_id") or program_id[:3]).strip()
        faculty = str(meta.get("faculty_name") or meta.get("f_name") or "").strip()
        field_name = str(meta.get("field_name") or "").strip()
        program_name = str(meta.get("program_name") or meta.get("name_th") or "").strip()
        program_type = str(meta.get("program_type") or "ภาษาไทย ปกติ").strip()
        campus = str(meta.get("campus_name") or meta.get("campus") or "วิทยาเขตหลัก").strip()

        if not university:
            m_uni = re.search(r"ข้อมูลการรับสมัครของ\s*([^\s‹›\n]+)", full_text)
            if m_uni:
                university = m_uni.group(1).strip()

        m_breadcrumb = re.search(r"(คณะ[^\s›‹\n]+|วิทยาลัย[^\s›‹\n]+|สถาบัน[^\s›‹\n]+|สำนักวิชา[^\s›‹\n]+)\s*›\s*([^\n*]+)", full_text)
        if m_breadcrumb:
            if not faculty:
                faculty = m_breadcrumb.group(1).strip()
            if not field_name:
                field_name = m_breadcrumb.group(2).strip().split(" หลักสูตร")[0].strip()

        if not program_name:
            h_tag = soup.find(["h1", "h2"])
            if h_tag:
                program_name = h_tag.get_text(" ", strip=True)
            elif soup.title:
                program_name = soup.title.get_text(" ", strip=True).replace("TCAS", "").strip()

        display_name = field_name if (field_name and field_name not in program_name) else (program_name or f"หลักสูตร {program_id}")
        if not university and universities_map:
            university = universities_map.get(university_id, "ไม่ระบุมหาวิทยาลัย")

        def parse_round_seat(round_num: int, kw: str) -> int:
            m = re.search(rf"รอบ\s*{round_num}\s*{kw}[^\dรอบ]*?รับ\s*([0-9,]+)\s*คน", full_text, re.IGNORECASE)
            if m:
                return int(m.group(1).replace(",", ""))
            m2 = re.search(rf"{round_num}\s*{kw}\s*รับ\s*([0-9,]+)\s*คน", full_text, re.IGNORECASE)
            if m2:
                return int(m2.group(1).replace(",", ""))
            return int(meta.get(f"r{round_num}", 0) or 0)

        r1 = parse_round_seat(1, "Portfolio")
        r2 = parse_round_seat(2, "Quota")
        r3 = parse_round_seat(3, "Admission")
        r4 = parse_round_seat(4, "Direct")
        total_seats = r1 + r2 + r3 + r4

        # ดึงยอดผู้เข้าชม
        views = 0
        for pattern in VIEW_COUNT_PATTERNS:
            m_view = pattern.search(full_text) or pattern.search(html)
            if m_view:
                raw_num = m_view.group(1).replace(",", "").strip()
                if raw_num.isdigit():
                    views = int(raw_num)
                    break

        if views == 0 and meta.get("views"):
            try:
                views = int(str(meta["views"]).replace(",", ""))
            except ValueError:
                pass

        return {
            "program_id": program_id,
            "program_name": display_name,
            "faculty": faculty or "ไม่ระบุคณะ",
            "university": university or "ไม่ระบุสถาบัน",
            "university_id": university_id,
            "campus": campus,
            "program_type": program_type,
            "category": classify_faculty_group(faculty, display_name),
            "r1": r1,
            "r2": r2,
            "r3": r3,
            "r4": r4,
            "total_seats": total_seats,
            "views": views,
            "url": f"{BASE_URL}/programs/{program_id}",
        }

async def scrape_all_programs(
    session: aiohttp.ClientSession,
    program_ids: List[str],
    prefetched_meta: Dict[str, Dict],
    universities_map: Dict[str, str],
) -> List[Dict[str, Any]]:
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    results: List[Dict[str, Any]] = []
    total = len(program_ids)
    logger.info(f"🚀 [Stage 2] เริ่มดึงยอดวิว {total:,} หลักสูตร...")
    start_ts = time.time()

    batch_size = 400
    for start_idx in range(0, total, batch_size):
        batch_ids = program_ids[start_idx : start_idx + batch_size]

        async def process_one(pid: str):
            html = await fetch_with_retry(session, f"{BASE_URL}/programs/{pid}", semaphore)
            if not html:
                return None
            return ProgramParser.parse_program_html(
                pid, html, prefetched_meta.get(pid), universities_map
            )

        batch_data = await asyncio.gather(*(process_one(pid) for pid in batch_ids))
        for item in batch_data:
            if item:
                results.append(item)

        elapsed = time.time() - start_ts
        done = min(start_idx + batch_size, total)
        speed = done / elapsed if elapsed > 0 else 0
        logger.info(f"ความคืบหน้า: {done:,}/{total:,} ({done/total*100:.1f}%) | ดึงได้: {len(results):,} | {speed:.1f} สาขา/วินาที")

    return results

def compile_and_save(programs: List[Dict[str, Any]]) -> None:
    logger.info("📊 [Stage 3] กำลังจัดอันดับและบันทึกไฟล์...")

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
            "total_universities": len({p["university"] for p in programs}),
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

    logger.info(f"🎉 สำเร็จ! บันทึกข้อมูล {len(programs):,} สาขาลง data.json เรียบร้อย")

async def main() -> None:
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS, ttl_dns_cache=300, ssl=False)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(connector=connector) as session:
        discovery = MasterCatalogDiscovery(session, semaphore)
        program_ids = await discovery.run_discovery()

        if not program_ids:
            logger.error("❌ ไม่พบรหัสหลักสูตร")
            return

        programs = await scrape_all_programs(
            session=session,
            program_ids=program_ids,
            prefetched_meta=discovery.prefetched_meta,
            universities_map=discovery.universities,
        )

        if programs:
            compile_and_save(programs)

if __name__ == "__main__":
    asyncio.run(main())
