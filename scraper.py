#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
myTCAS ViewTracker - Nationwide Course Scraper & Data Pipeline
รองรับการค้นหา Program ID ทั้งประเทศอัตโนมัติ (Discovery) + Async Batch Scraping
"""

import asyncio
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any

import aiohttp
from bs4 import BeautifulSoup

# ตั้งค่า Timezone ประเทศไทย (UTC+7)
TZ_TH = timezone(timedelta(hours=7))

# ตั้งค่า Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("myTCAS-Scraper")

# ---------------------------------------------------------------------------
# ค่าคงที่และการตั้งค่า (Configuration)
# ---------------------------------------------------------------------------
BASE_URL = "https://course.mytcas.com"
UNIVERSITIES_URL = f"{BASE_URL}/universities"

# Endpoint JSON ภายในที่ระบบ myTCAS (S3 / CDN / API) มักใช้เก็บสารบัญหลักสูตร
CANDIDATE_JSON_ENDPOINTS = [
    "https://my-tcas.s3.ap-southeast-1.amazonaws.com/mytcas/courses.json",
    "https://my-tcas.s3.ap-southeast-1.amazonaws.com/mytcas/programs.json",
    "https://my-tcas.s3.ap-southeast-1.amazonaws.com/mytcas/universities.json",
    f"{BASE_URL}/api/universities",
    f"{BASE_URL}/api/programs",
]

MAX_CONCURRENT_REQUESTS = 30     # จำนวน Worker ขนานกัน (ปรับให้เหมาะกับ Rate Limit)
REQUEST_TIMEOUT_SEC = 25         # Timeout ต่อ 1 คำขอ
MAX_RETRIES = 4                  # จำนวนครั้งที่ลองใหม่เมื่อติด Rate Limit / Network Error
OUTPUT_FILE = Path("data.json")
SPLIT_DIR = Path("data/universities")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
]

# รูปแบบมาตรฐานของรหัสหลักสูตร myTCAS (เช่น 10010121300599A หรือตัวเลข/ตัวอักษร 10-20 หลัก)
PROGRAM_ID_PATTERN = re.compile(r"/programs/([0-9A-Za-z_\-]{8,25})")
UNIVERSITY_ID_PATTERN = re.compile(r"/universities/([0-9A-Za-z_\-]{2,10})")

# Strict Regex สำหรับดึงยอดผู้เข้าชม (ป้องกันการหยิบเลข 1 จาก "รอบ 1 Portfolio")
VIEW_COUNT_PATTERNS = [
    re.compile(r"ยอดผู้เข้าชม\s*[:\-]?\s*([0-9,]+)\s*ครั้ง", re.IGNORECASE),
    re.compile(r"ผู้เข้าชม\s*[:\-]?\s*([0-9,]+)\s*ครั้ง", re.IGNORECASE),
    re.compile(r'"(?:views|view_count|viewCount|hits|pageViews)"\s*:\s*([0-9]+)', re.IGNORECASE),
]


def get_headers(referer: str = BASE_URL) -> Dict[str, str]:
    """สร้าง HTTP Headers จำลองการเปิดจากเบราว์เซอร์ของผู้ใช้ในประเทศไทย"""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json,*/*;q=0.8",
        "Accept-Language": "th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": referer,
        "Origin": BASE_URL,
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Cache-Control": "no-cache",
    }


def classify_faculty_group(faculty: str, program_name: str) -> str:
    """จัดหมวดหมู่กลุ่มคณะ/สาขาอัตโนมัติสำหรับตัวกรองบนเว็บไซต์"""
    text = f"{faculty} {program_name}".lower()
    if any(k in text for k in ["แพทยศาสตร", "ทันตแพทย", "เภสัชศาสตร", "สัตวแพทย"]):
        return "แพทย์ / ทันตะ / เภสัช / สัตวแพทย์"
    if any(k in text for k in ["พยาบาล", "สหเวช", "เทคนิคการแพทย์", "สาธารณสุข", "กายภาพบำบัด", "รังสีเทคนิค"]):
        return "พยาบาล / สหเวช / สุขภาพ"
    if any(k in text for k in ["คอมพิวเตอร์", "ดิจิทัล", "สารสนเทศ", "ปัญญาประดิษฐ์", "ข้อมูล", "ไซเบอร์", "ซอฟต์แวร์", "ai", "data", "cyber"]):
        return "วิทยาการคอม / IT / ดิจิทัล"
    if any(k in text for k in ["วิศวกรรม", "ช่างอุตสาหกรรม", "เทคโนโลยีวิศวกรรม"]):
        return "วิศวกรรมศาสตร์"
    if any(k in text for k in ["บริหารธุรกิจ", "บัญชี", "พาณิชยศาสตร์", "การตลาด", "การจัดการ", "โลจิสติกส์", "การเงิน"]):
        return "บริหารธุรกิจ / บัญชี"
    if "เศรษฐศาสตร" in text:
        return "เศรษฐศาสตร์"
    if any(k in text for k in ["นิติศาสตร", "กฎหมาย", "รัฐศาสตร", "การปกครอง", "ความสัมพันธ์ระหว่างประเทศ", "รัฐประศาสน"]):
        return "นิติศาสตร์ / รัฐศาสตร์"
    if any(k in text for k in ["อักษรศาสตร", "ศิลปศาสตร", "มนุษยศาสตร", "ภาษา", "โบราณคดี", "สังคมศาสตร"]):
        return "มนุษยศาสตร์ / ศิลปศาสตร์ / ภาษา"
    if any(k in text for k in ["ครุศาสตร", "ศึกษาศาสตร", "การสอน", "ครู"]):
        return "ครุศาสตร์ / ศึกษาศาสตร์"
    if any(k in text for k in ["สถาปัตยกรรม", "ศิลปกรรม", "มัณฑนศิลป์", "วิจิตรศิลป์", "นิเทศศาสตร", "วารสารศาสตร", "สื่อสารมวลชน", "การออกแบบ"]):
        return "สถาปัตย์ / ศิลปกรรม / นิเทศศาสตร์"
    if any(k in text for k in ["เกษตร", "ประมง", "วนศาสตร", "อุตสาหกรรมเกษตร", "เทคโนโลยีการอาหาร"]):
        return "เกษตรศาสตร์ / อุตสาหกรรมเกษตร"
    if "วิทยาศาสตร" in text:
        return "วิทยาศาสตร์"
    return "อื่นๆ / สหวิทยาการ"


# ---------------------------------------------------------------------------
# HTTP Client พร้อมระบบ Rate Limit & Retry (Exponential Backoff + Jitter)
# ---------------------------------------------------------------------------
async def fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
    expect_json: bool = False,
) -> Optional[Any]:
    """ดึงข้อมูลจาก URL พร้อมจัดการ Concurrency, Jitter และ Exponential Backoff"""
    for attempt in range(1, MAX_RETRIES + 1):
        async with semaphore:
            # Jitter เล็กน้อยเพื่อกระจายโหลดไม่ให้ชน WAF Rate Limit
            await asyncio.sleep(random.uniform(0.04, 0.18))
            try:
                async with session.get(
                    url,
                    headers=get_headers(),
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SEC),
                ) as response:
                    if response.status == 200:
                        if expect_json:
                            return await response.json(content_type=None)
                        return await response.text(errors="replace")

                    if response.status in (429, 500, 502, 503, 504):
                        wait_time = (2 ** attempt) + random.uniform(0.5, 2.0)
                        logger.warning(
                            f"HTTP {response.status} ที่ {url} (รอบ {attempt}/{MAX_RETRIES}) รอ {wait_time:.1f}s..."
                        )
                        await asyncio.sleep(wait_time)
                        continue

                    if response.status == 404:
                        return None

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                wait_time = (1.8 ** attempt) + random.uniform(0.2, 1.0)
                if attempt == MAX_RETRIES:
                    logger.debug(f"ดึงข้อมูลไม่สำเร็จหลังจากลอง {MAX_RETRIES} ครั้ง: {url} ({exc})")
                else:
                    await asyncio.sleep(wait_time)
    return None


# ---------------------------------------------------------------------------
# STAGE 1: Discovery - ค้นหาสถาบันและรหัสหลักสูตรทั้งหมดอัตโนมัติ
# ---------------------------------------------------------------------------
class MasterCatalogDiscovery:
    def __init__(self, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore):
        self.session = session
        self.semaphore = semaphore
        self.universities: Dict[str, str] = {}     # uni_id -> uni_name
        self.program_ids: Set[str] = set()         # รหัสหลักสูตรทั้งหมด
        self.prefetched_meta: Dict[str, Dict] = {} # ข้อมูลพื้นฐานที่ได้จาก JSON (ถ้ามี)

    async def run_discovery(self) -> List[str]:
        logger.info("🔍 [Stage 1] เริ่มกระบวนการค้นหาหลักสูตรทั้งหมดทั่วประเทศ (Discovery)...")

        # วิธีที่ 1: ตรวจสอบ Static JSON / Internal API ของ myTCAS ก่อน
        await self._probe_json_endpoints()

        # วิธีที่ 2: กวาดจากหน้า /universities และหน้าย่อยของแต่ละมหาวิทยาลัย
        await self._crawl_universities_and_programs()

        # วิธีที่ 3: หากหน้าเว็บเรนเดอร์ฝั่ง Client ล้วน (SPA) จนได้รหัสมาไม่ครบ ให้ใช้ Playwright กวาดสารบัญ
        if len(self.program_ids) < 500:
            logger.info(
                f"พบหลักสูตรจาก Static HTML เพียง {len(self.program_ids)} รายการ -> เปิดโหมด Playwright Discovery..."
            )
            await self._discover_via_playwright()

        logger.info(
            f"✅ [Stage 1 เสร็จสมบูรณ์] พบมหาวิทยาลัย {len(self.universities)} แห่ง | "
            f"พบหลักสูตรทั้งหมด {len(self.program_ids):,} สาขา"
        )
        return sorted(self.program_ids)

    async def _probe_json_endpoints(self) -> None:
        """ตรวจสอบว่า myTCAS เปิด JSON Catalog ไว้หรือไม่ เพื่อดึงรายชื่อสาขาทั้งหมดอย่างรวดเร็ว"""
        for endpoint in CANDIDATE_JSON_ENDPOINTS:
            data = await fetch_with_retry(self.session, endpoint, self.semaphore, expect_json=True)
            if not data:
                continue

            if isinstance(data, list):
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    # กรณีเป็นรายการมหาวิทยาลัย
                    u_id = str(item.get("u_id") or item.get("university_id") or item.get("id") or "").strip()
                    u_name = str(item.get("u_name") or item.get("university_name") or item.get("name_th") or "").strip()
                    if u_id and u_name and len(u_id) <= 6:
                        self.universities[u_id] = u_name

                    # กรณีเป็นรายการหลักสูตร
                    p_id = str(item.get("program_id") or item.get("id") or "").strip()
                    if len(p_id) >= 8:
                        self.program_ids.add(p_id)
                        self.prefetched_meta[p_id] = item

            logger.info(
                f"ดึงข้อมูลตั้งต้นจาก {endpoint}: มหาวิทยาลัย {len(self.universities)} แห่ง, หลักสูตร {len(self.program_ids)} สาขา"
            )

    def _extract_links_and_next_data(self, html: str) -> Tuple[Set[str], Set[str]]:
        """แกะรหัสมหาวิทยาลัยและรหัสหลักสูตรจากทั้ง HTML DOM และ __NEXT_DATA__ JSON"""
        found_unis: Set[str] = set()
        found_progs: Set[str] = set()

        for match in UNIVERSITY_ID_PATTERN.finditer(html):
            u_id = match.group(1)
            if u_id not in ("universities", "programs", "api", "_next"):
                found_unis.add(u_id)

        for match in PROGRAM_ID_PATTERN.finditer(html):
            p_id = match.group(1)
            if p_id not in ("universities", "programs", "api", "_next"):
                found_progs.add(p_id)

        # ค้นหาเพิ่มเติมใน JSON ของ Next.js (__NEXT_DATA__)
        soup = BeautifulSoup(html, "lxml")
        for a_tag in soup.select('a[href*="/universities/"]'):
            href = a_tag.get("href", "")
            m = UNIVERSITY_ID_PATTERN.search(href)
            if m:
                u_id = m.group(1)
                found_unis.add(u_id)
                name_text = a_tag.get_text(strip=True)
                if name_text and u_id not in self.universities:
                    self.universities[u_id] = name_text

        next_script = soup.find("script", id="__NEXT_DATA__")
        if next_script and next_script.string:
            try:
                payload = json.loads(next_script.string)
                raw_str = json.dumps(payload, ensure_ascii=False)
                for m in re.finditer(r'"program_id"\s*:\s*"([0-9A-Za-z_\-]{8,25})"', raw_str):
                    found_progs.add(m.group(1))
                for m in re.finditer(r'"([0-9]{14}[A-Z0-9])"', raw_str):
                    found_progs.add(m.group(1))
            except Exception:
                pass

        return found_unis, found_progs

    async def _crawl_universities_and_programs(self) -> None:
        """กวาดหน้าสารบัญมหาวิทยาลัย และแตกแขนงเข้าทุกมหาวิทยาลัยเพื่อเก็บรหัสหลักสูตร"""
        html = await fetch_with_retry(self.session, UNIVERSITIES_URL, self.semaphore)
        if html:
            unis, progs = self._extract_links_and_next_data(html)
            for u in unis:
                self.universities.setdefault(u, "")
            self.program_ids.update(progs)

        # เสริมรหัสมหาวิทยาลัยมาตรฐานในระบบ ทปอ. (001 - 199 และรหัสสถาบันสมทบ) เผื่อบางแห่งซ่อนในหน้าแรก
        if len(self.universities) < 40:
            for code in range(1, 130):
                self.universities.setdefault(f"{code:03d}", "")

        uni_ids = sorted(self.universities.keys())
        logger.info(f"กำลังสแกนหน้ามหาวิทยาลัยทั้งหมด {len(uni_ids)} รหัสแบบ Async...")

        tasks = [
            fetch_with_retry(self.session, f"{UNIVERSITIES_URL}/{u_id}", self.semaphore)
            for u_id in uni_ids
        ]
        results = await asyncio.gather(*tasks)

        faculty_urls: Set[str] = set()
        for u_id, uni_html in zip(uni_ids, results):
            if not uni_html:
                continue
            _, progs = self._extract_links_and_next_data(uni_html)
            self.program_ids.update(progs)

            # เก็บลิงก์ระดับคณะ/สาขา (ถ้ามีหน้าคั่นกลาง เช่น /universities/001/faculties/...)
            for m in re.finditer(r'href=["\'](/universities/[^"\']+)["\']', uni_html):
                sub_path = m.group(1)
                if sub_path.count("/") >= 3:
                    faculty_urls.add(f"{BASE_URL}{sub_path}")

        if faculty_urls:
            logger.info(f"กำลังสแกนหน้าคณะย่อยเพิ่มเติมจำนวน {len(faculty_urls)} หน้า...")
            fac_tasks = [
                fetch_with_retry(self.session, url, self.semaphore)
                for url in sorted(faculty_urls)
            ]
            fac_results = await asyncio.gather(*fac_tasks)
            for fac_html in fac_results:
                if fac_html:
                    _, progs = self._extract_links_and_next_data(fac_html)
                    self.program_ids.update(progs)

    async def _discover_via_playwright(self) -> None:
        """
        ใช้ Playwright ดักจับ JSON ของแอป myTCAS และกวาดลิงก์กรณีที่เว็บทำงานแบบ SPA
        (บล็อกรูปภาพและฟอนต์ทั้งหมดเพื่อความเร็วสูงสุด)
        """
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.warning("ไม่ได้ติดตั้ง Playwright ข้ามขั้นตอน Playwright Discovery")
            return

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            context = await browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                locale="th-TH",
                timezone_id="Asia/Bangkok",
            )
            page = await context.new_page()

            # ดักจับทุก JSON Response ที่เบราว์เซอร์คุยกับ Backend ของ myTCAS
            async def handle_response(response):
                try:
                    if "json" in response.headers.get("content-type", "").lower():
                        text = await response.text()
                        for m in re.finditer(r'"([0-9]{13,15}[A-Z0-9]?)"', text):
                            self.program_ids.add(m.group(1))
                        for m in PROGRAM_ID_PATTERN.finditer(text):
                            self.program_ids.add(m.group(1))
                        # ถ้าเจอ JSON ก้อนใหญ่ที่มีรายการหลักสูตรหรือมหาวิทยาลัย ให้เก็บเข้าเมมโมรีทันที
                        data = json.loads(text)
                        if isinstance(data, list) and len(data) > 20:
                            for row in data:
                                if isinstance(row, dict):
                                    pid = str(row.get("program_id") or row.get("id") or "")
                                    uid = str(row.get("u_id") or row.get("university_id") or "")
                                    uname = str(row.get("u_name") or row.get("university_name") or "")
                                    if uid and uname:
                                        self.universities[uid] = uname
                                    if len(pid) >= 8:
                                        self.program_ids.add(pid)
                                        self.prefetched_meta[pid] = row
                except Exception:
                    pass

            page.on("response", handle_response)
            await page.route(
                "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,css}",
                lambda route: route.abort(),
            )

            try:
                await page.goto(UNIVERSITIES_URL, wait_until="networkidle", timeout=30000)
                content = await page.content()
                unis, progs = self._extract_links_and_next_data(content)
                for u in unis:
                    self.universities.setdefault(u, "")
                self.program_ids.update(progs)

                # หากยังได้หลักสูตรไม่ครบ ให้เปิดหน้ามหาวิทยาลัยแบบขนาน 8 แท็บ
                if len(self.program_ids) < 1000 and self.universities:
                    uni_list = sorted(self.universities.keys())
                    logger.info(f"กวาดหน้ามหาวิทยาลัย {len(uni_list)} แห่งผ่าน Playwright Workers...")
                    sem = asyncio.Semaphore(8)

                    async def visit_uni(u_id: str):
                        async with sem:
                            pg = await context.new_page()
                            pg.on("response", handle_response)
                            await pg.route(
                                "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,css}",
                                lambda route: route.abort(),
                            )
                            try:
                                await pg.goto(f"{UNIVERSITIES_URL}/{u_id}", wait_until="networkidle", timeout=20000)
                                html_u = await pg.content()
                                _, p_set = self._extract_links_and_next_data(html_u)
                                self.program_ids.update(p_set)
                            except Exception:
                                pass
                            finally:
                                await pg.close()

                    await asyncio.gather(*(visit_uni(u) for u in uni_list))
            finally:
                await browser.close()


# ---------------------------------------------------------------------------
# STAGE 2: Batch Scraping - ดึงรายละเอียดและยอดผู้เข้าชมของทุกหลักสูตร
# ---------------------------------------------------------------------------
class ProgramParser:
    """ตัวแยกข้อมูลหลักสูตรและยอดผู้เข้าชม (ป้องกันค่าเป็น 0 หรือหลุดเป็นเลข 1)"""

    @staticmethod
    def parse_program_html(
        program_id: str,
        html: str,
        fallback_meta: Optional[Dict] = None,
        universities_map: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        soup = BeautifulSoup(html, "lxml")
        full_text = soup.get_text(" ", strip=True)

        # ตรวจสอบว่าหน้าเว็บมีอยู่จริงหรือไม่
        if "ไม่พบข้อมูล" in full_text and len(full_text) < 500:
            return None

        meta = fallback_meta or {}
        university = str(meta.get("u_name") or meta.get("university_name") or "").strip()
        university_id = str(meta.get("u_id") or meta.get("university_id") or program_id[:3]).strip()
        faculty = str(meta.get("faculty_name") or meta.get("f_name") or "").strip()
        field_name = str(meta.get("field_name") or "").strip()
        program_name = str(meta.get("program_name") or meta.get("name_th") or "").strip()
        program_name_en = str(meta.get("program_name_en") or meta.get("name_en") or "").strip()
        program_type = str(meta.get("program_type") or "ภาษาไทย ปกติ").strip()
        campus = str(meta.get("campus_name") or meta.get("campus") or "วิทยาเขตหลัก").strip()

        # 1. แกะจาก Breadcrumb / Header ของหน้า course.mytcas.com/programs/{id}
        # ตัวอย่างรูปแบบจริง: "ข้อมูลการรับสมัครของจุฬาลงกรณ์มหาวิทยาลัย จุฬาลงกรณ์มหาวิทยาลัย คณะวิศวกรรมศาสตร์›วิศวกรรมคอมพิวเตอร์"
        if not university:
            m_uni = re.search(r"ข้อมูลการรับสมัครของ\s*([^\s‹›\n]+)", full_text)
            if m_uni:
                university = m_uni.group(1).strip()

        # แกะคณะและสาขาจากเครื่องหมาย ›
        m_breadcrumb = re.search(r"(คณะ[^\s›‹\n]+|วิทยาลัย[^\s›‹\n]+|สถาบัน[^\s›‹\n]+|สำนักวิชา[^\s›‹\n]+)\s*›\s*([^\n*]+)", full_text)
        if m_breadcrumb:
            if not faculty:
                faculty = m_breadcrumb.group(1).strip()
            if not field_name:
                field_name = m_breadcrumb.group(2).strip().split(" หลักสูตร")[0].strip()

        # 2. แกะจากตารางรายละเอียด (dt / dd หรือโครงสร้าง Text)
        def extract_field(label: str, stop_labels: List[str]) -> str:
            stop_expr = "|".join(re.escape(s) for s in stop_labels)
            pattern = rf"{re.escape(label)}\s+(.+?)\s+(?:{stop_expr})"
            m = re.search(pattern, full_text)
            return m.group(1).strip() if m else ""

        if not program_name:
            program_name = extract_field(
                "ชื่อหลักสูตร",
                ["ชื่อหลักสูตรภาษาอังกฤษ", "ประเภทหลักสูตร", "วิทยาเขต", "ค่าใช้จ่าย", "รอบ 1"],
            )
        if not program_name_en:
            program_name_en = extract_field(
                "ชื่อหลักสูตรภาษาอังกฤษ",
                ["ประเภทหลักสูตร", "วิทยาเขต", "ค่าใช้จ่าย", "รอบ 1"],
            )
        extracted_type = extract_field(
            "ประเภทหลักสูตร",
            ["วิทยาเขต", "ค่าใช้จ่าย", "อัตราการสำเร็จ", "รอบ 1"],
        )
        if extracted_type:
            program_type = extracted_type

        extracted_campus = extract_field(
            "วิทยาเขต",
            ["ค่าใช้จ่าย", "อัตราการสำเร็จ", "ค่ามัธยฐาน", "รอบ 1"],
        )
        if extracted_campus:
            campus = extracted_campus

        # สำรองชื่อหลักสูตรจาก <title> หรือ <h1>, <h2>
        if not program_name:
            h_tag = soup.find(["h1", "h2"])
            if h_tag:
                program_name = h_tag.get_text(" ", strip=True)
            elif soup.title:
                program_name = soup.title.get_text(" ", strip=True).replace("TCAS", "").strip()

        if field_name and field_name not in program_name:
            display_name = f"{field_name} ({program_name})" if program_name else field_name
        else:
            display_name = program_name or field_name or f"หลักสูตร {program_id}"

        if not university and universities_map:
            university = universities_map.get(university_id, "ไม่ระบุมหาวิทยาลัย")

        # 3. ดึงจำนวนรับสมัครทุกรอบ (รอบ 1 Portfolio, รอบ 2 Quota, รอบ 3 Admission, รอบ 4 Direct Admission)
        def parse_round_seat(round_num: int, round_keyword: str) -> int:
            pattern = rf"รอบ\s*{round_num}\s*{round_keyword}[^\dรอบ]*?รับ\s*([0-9,]+)\s*คน"
            m = re.search(pattern, full_text, re.IGNORECASE)
            if m:
                return int(m.group(1).replace(",", ""))
            # รูปแบบสำรอง: "1Portfolioรับ 225 คน"
            pattern_alt = rf"{round_num}\s*{round_keyword}\s*รับ\s*([0-9,]+)\s*คน"
            m2 = re.search(pattern_alt, full_text, re.IGNORECASE)
            if m2:
                return int(m2.group(1).replace(",", ""))
            return int(meta.get(f"r{round_num}", 0) or 0)

        r1 = parse_round_seat(1, "Portfolio")
        r2 = parse_round_seat(2, "Quota")
        r3 = parse_round_seat(3, "Admission")
        r4 = parse_round_seat(4, "Direct")
        total_seats = r1 + r2 + r3 + r4

        # 4. ดึงตัวเลข "ยอดผู้เข้าชม" อย่างแม่นยำ (ไม่หยิบเลข 1 จาก รอบ 1 Portfolio เด็ดขาด)
        views = 0
        for pattern in VIEW_COUNT_PATTERNS:
            m_view = pattern.search(full_text) or pattern.search(html)
            if m_view:
                raw_num = m_view.group(1).replace(",", "").strip()
                if raw_num.isdigit():
                    views = int(raw_num)
                    break

        # ตรวจสอบใน __NEXT_DATA__ หากยังไม่พบใน HTML ปกติ
        if views == 0:
            next_script = soup.find("script", id="__NEXT_DATA__")
            if next_script and next_script.string:
                for pattern in VIEW_COUNT_PATTERNS:
                    m_view = pattern.search(next_script.string)
                    if m_view:
                        views = int(m_view.group(1).replace(",", ""))
                        break

        if views == 0 and meta.get("views"):
            try:
                views = int(str(meta["views"]).replace(",", ""))
            except ValueError:
                pass

        category = classify_faculty_group(faculty, display_name)

        return {
            "program_id": program_id,
            "program_name": display_name,
            "program_name_en": program_name_en,
            "faculty": faculty or "ไม่ระบุคณะ",
            "university": university or "ไม่ระบุสถาบัน",
            "university_id": university_id,
            "campus": campus,
            "program_type": program_type,
            "category": category,
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
    """[Stage 2] กวาดข้อมูลทุกหลักสูตรแบบขนานด้วย Async Workers"""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    results: List[Dict[str, Any]] = []
    zero_view_Programs: List[Dict[str, Any]] = []

    total = len(program_ids)
    logger.info(f"🚀 [Stage 2] เริ่มดึงข้อมูลและยอดวิวของ {total:,} หลักสูตร (Concurrency={MAX_CONCURRENT_REQUESTS})...")
    start_ts = time.time()

    # แบ่ง Batch ละ 500 รายการเพื่อรายงานความคืบหน้าและจัดการ Memory
    batch_size = 500
    for start_idx in range(0, total, batch_size):
        batch_ids = program_ids[start_idx : start_idx + batch_size]

        async def process_one(pid: str) -> Optional[Dict[str, Any]]:
            url = f"{BASE_URL}/programs/{pid}"
            html = await fetch_with_retry(session, url, semaphore)
            if not html:
                return None
            return ProgramParser.parse_program_html(
                pid,
                html,
                fallback_meta=prefetched_meta.get(pid),
                universities_map=universities_map,
            )

        batch_data = await asyncio.gather(*(process_one(pid) for pid in batch_ids))
        for item in batch_data:
            if item:
                results.append(item)
                if item["views"] == 0:
                    zero_view_Programs.append(item)

        elapsed = time.time() - start_ts
        done = min(start_idx + batch_size, total)
        speed = done / elapsed if elapsed > 0 else 0
        logger.info(
            f"ความคืบหน้า: {done:,}/{total:,} ({done/total*100:.1f}%) | "
            f"สำเร็จ: {len(results):,} | ความเร็วเฉลี่ย: {speed:.1f} สาขา/วินาที"
        )

    # หากพบว่ามีสาขาที่ยอดวิวเป็น 0 (อาจเกิดจากตัวเลขโหลดผ่าน Client XHR API) ให้สุ่ม/ซ่อมด้วย Playwright
    if zero_view_Programs and len(zero_view_Programs) <= 300:
        logger.info(f"กำลังซ่อมแซมยอดวิวที่รอ Hydration จำนวน {len(zero_view_Programs)} สาขาด้วย Playwright...")
        await repair_zero_views_with_playwright(zero_view_Programs)

    return results


async def repair_zero_views_with_playwright(items: List[Dict[str, Any]]) -> None:
    """ใช้ Headless Browser ดึงยอดวิวเฉพาะหลักสูตรที่ต้องรอ Client-Side JS Hydration"""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = await browser.new_context(user_agent=random.choice(USER_AGENTS), locale="th-TH")
        sem = asyncio.Semaphore(6)

        async def fix_item(item: Dict[str, Any]):
            async with sem:
                page = await context.new_page()
                await page.route(
                    "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,css}",
                    lambda route: route.abort(),
                )
                try:
                    await page.goto(item["url"], wait_until="domcontentloaded", timeout=15000)
                    try:
                        await page.wait_for_function(
                            "() => document.body.innerText.includes('ยอดผู้เข้าชม')",
                            timeout=5000,
                        )
                    except Exception:
                        pass
                    text = await page.inner_text("body")
                    for pattern in VIEW_COUNT_PATTERNS:
                        m = pattern.search(text)
                        if m:
                            val = int(m.group(1).replace(",", ""))
                            if val > 0:
                                item["views"] = val
                                break
                except Exception:
                    pass
                finally:
                    await page.close()

        await asyncio.gather(*(fix_item(it) for it in items))
        await browser.close()


# ---------------------------------------------------------------------------
# STAGE 3: Data Compilation - เปรียบเทียบยอดวิว จัดอันดับ และบันทึกไฟล์
# ---------------------------------------------------------------------------
def load_previous_views(filepath: Path) -> Dict[str, Tuple[int, int]]:
    """โหลดยอดวิวและอันดับของรอบก่อนหน้าจาก data.json เพื่อคำนวณการเปลี่ยนแปลง (+Diff)"""
    prev_map: Dict[str, Tuple[int, int]] = {}
    if not filepath.exists():
        return prev_map
    try:
        with filepath.open("r", encoding="utf-8") as f:
            old_payload = json.load(f)
        old_programs = old_payload.get("programs", [])
        for row in old_programs:
            pid = row.get("program_id")
            if pid:
                prev_map[pid] = (int(row.get("views", 0)), int(row.get("rank", 0)))
    except Exception as exc:
        logger.warning(f"ไม่สามารถอ่านไฟล์ data.json เดิมได้: {exc}")
    return prev_map


def compile_and_save(programs: List[Dict[str, Any]]) -> None:
    """[Stage 3] จัดเรียงลำดับตามยอดวิว คำนวณค่าสถิติ และบันทึกไฟล์ JSON"""
    logger.info("📊 [Stage 3] กำลังประมวลผล จัดอันดับ และบันทึกข้อมูล...")

    prev_map = load_previous_views(OUTPUT_FILE)

    # จัดเรียงตามยอดวิวจากมากไปน้อย (หากเท่ากันให้เรียงตามจำนวนรับรวม และรหัสสาขา)
    programs.sort(key=lambda x: (x["views"], x["total_seats"], x["program_id"]), reverse=True)

    total_views = 0
    total_seats_all = 0
    by_university: Dict[str, List[Dict[str, Any]]] = {}

    for idx, item in enumerate(programs, start=1):
        item["rank"] = idx
        pid = item["program_id"]
        prev_views, prev_rank = prev_map.get(pid, (item["views"], idx))

        item["views_prev"] = prev_views
        item["views_diff"] = max(0, item["views"] - prev_views)
        item["rank_change"] = (prev_rank - idx) if prev_rank > 0 else 0

        total_views += item["views"]
        total_seats_all += item["total_seats"]

        u_id = item.get("university_id") or "other"
        by_university.setdefault(u_id, []).append(item)

    now_th = datetime.now(TZ_TH)
    cycle_label = "รอบเช้า (06:00 น.)" if now_th.hour < 12 else "รอบเย็น (18:00 น.)"

    payload = {
        "metadata": {
            "updated_at": now_th.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_iso": now_th.isoformat(),
            "cycle": cycle_label,
            "total_programs": len(programs),
            "total_universities": len({p["university"] for p in programs}),
            "total_views": total_views,
            "total_seats": total_seats_all,
        },
        "programs": programs,
    }

    # บันทึกไฟล์หลัก data.json (ใช้ separators เพื่อบีบขนาดไฟล์ให้โหลดบนเว็บได้เร็ว)
    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    # บันทึกไฟล์แยกตามมหาวิทยาลัยเผื่อกรณีนำไปต่อยอด API ย่อย
    SPLIT_DIR.mkdir(parents=True, exist_ok=True)
    for u_id, uni_progs in by_university.items():
        safe_id = re.sub(r"[^0-9A-Za-z_\-]", "_", u_id)
        uni_file = SPLIT_DIR / f"{safe_id}.json"
        with uni_file.open("w", encoding="utf-8") as f:
            json.dump(uni_progs, f, ensure_ascii=False, separators=(",", ":"))

    size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
    logger.info(
        f"🎉 บันทึกไฟล์ {OUTPUT_FILE} สำเร็จ! "
        f"(รวม {len(programs):,} สาขา | ขนาดไฟล์ {size_mb:.2f} MB)"
    )


# ---------------------------------------------------------------------------
# Main Entrypoint
# ---------------------------------------------------------------------------
async def main() -> None:
    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT_REQUESTS,
        ttl_dns_cache=300,
        ssl=False,
    )
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(connector=connector) as session:
        # Stage 1: Discovery
        discovery = MasterCatalogDiscovery(session, semaphore)
        program_ids = await discovery.run_discovery()

        if not program_ids:
            raise RuntimeError("ไม่พบรหัสหลักสูตรใดๆ จากระบบ myTCAS กรุณาตรวจสอบการเชื่อมต่อ")

        # Stage 2: Batch Scraping
        programs = await scrape_all_programs(
            session=session,
            program_ids=program_ids,
            prefetched_meta=discovery.prefetched_meta,
            universities_map=discovery.universities,
        )

        if not programs:
            raise RuntimeError("ดึงข้อมูลหลักสูตรได้ 0 รายการ ยกเลิกการเขียนทับไฟล์ data.json")

        # Stage 3: Compilation & Output
        compile_and_save(programs)


if __name__ == "__main__":
    asyncio.run(main())
