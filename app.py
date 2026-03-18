from flask import Flask, request, jsonify
from dotenv import load_dotenv
import anthropic
import requests as http_requests
import threading
import time
import os
import json
import html as html_mod
from datetime import datetime

load_dotenv()

app = Flask(__name__)

AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = "appomMTO8slCdUpZU"
TABLE_ID = "tblbJmeVyUdojeaMU"
AIRTABLE_URL = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"

anthropic_client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# In-memory job tracker
jobs = {}


def build_contact_prompt(name, reference_url):
    url_block = ""
    if reference_url and reference_url.strip():
        url_block = f" (reference: {reference_url.strip()})"

    return (
        f"Find ALL contact information for **{name}**{url_block}.\n\n"
        "Do this in ONE pass — run all searches in parallel and compile a complete dossier. "
        "Do not ask me any clarifying questions. Do not stop until you have exhausted every angle below.\n\n"
        "**AMBIGUITY RULE:** If no URL is provided and the name is common/ambiguous, first search to "
        "identify candidates. If there are 2+ plausible people, output the top 2-3 candidates with "
        "identifying info (role, org, location) and stop — do not guess. If there is clearly only one "
        "prominent match, proceed with the full lookup.\n\n"
        "## Step 0: Fetch the reference URL (do FIRST — this is the most important step)\n\n"
        "Use WebFetch to load the actual page at the reference URL. Scrape it for:\n"
        "- **Email addresses** (check mailto: links, footer, contact sections, \"Email\" links)\n"
        "- **Social media links** (Twitter, LinkedIn, GitHub, etc.)\n"
        "- **Role, org, affiliations** — to understand who this person is\n"
        "- **Any stated contact preferences**\n\n"
        "Many personal sites have the email right on the page as a hyperlink. Do NOT skip this step. "
        "If the URL is a Wikipedia page, also look for an \"Official website\" link in the external "
        "links section and fetch THAT too.\n\n"
        "After fetching, determine: Are they a celebrity/entertainer, academic, tech founder, public "
        "intellectual, politician, pseudonymous figure, or other? This determines which search branches "
        "matter most.\n\n"
        "## Part 1: Direct Contact (run all these searches in parallel, ALONGSIDE Step 0)\n\n"
        "Search for ALL of the following simultaneously:\n"
        f"1. \"{name}\" email contact\n"
        f"2. \"{name}\" \"@\" email address\n"
        f"3. \"{name}\" official website contact\n"
        f"4. \"{name}\" site:linkedin.com\n"
        f"5. \"{name}\" booking speaking contact phone\n"
        "6. site:youtube.com contact OR email\n\n"
        "From those results, follow up with:\n"
        "6. **If celebrity/entertainer:** Search for their talent agency (UTA/CAA/WME/ICM) page, "
        "agent name, and agent email.\n"
        "7. **If academic:** Search their university faculty page AND department contact page.\n"
        "8. **If pseudonymous:** Search all known handles across GitHub, LessWrong, EA Forum, personal blogs.\n"
        "9. **For everyone:** Search RocketReach / ContactOut / ZoomInfo for the person directly — "
        "note paywall URLs where email can be unlocked.\n\n"
        "## MANDATORY: Org Email Derivation (do this for EVERY org/company they are affiliated with)\n\n"
        "This step is NON-OPTIONAL. For every company, org, nonprofit, or institution the person is "
        "currently affiliated with:\n\n"
        "1. Identify the org's domain\n"
        "2. Search \"@[domain]\" email format site:rocketreach.co to find the pattern\n"
        "3. Derive their email by applying the pattern to their name\n"
        "4. Include the derived email in the output table\n\n"
        "Do this for EVERY current org — not just one.\n\n"
        "## Part 2: Gatekeeper / Routing Contacts (run in parallel with Part 1)\n\n"
        "Search simultaneously:\n"
        f"1. \"{name}\" assistant OR \"chief of staff\" OR scheduler contact\n"
        f"2. \"{name}\" team staff office manager\n"
        f"3. \"{name}\" publicist OR \"press contact\" OR \"media inquiries\"\n"
        "4. Search org team/staff pages for people who route communications to them.\n\n"
        "## Part 3: Social / DM Channels (run in parallel)\n\n"
        "1. Twitter/X handle\n"
        "2. LinkedIn profile URL\n"
        "3. YouTube (check About page for business inquiry email)\n"
        "4. Personal blog / website\n"
        "5. Substack / newsletter\n"
        "6. GitHub profile\n"
        "7. Instagram\n"
        "8. Personal website contact form\n\n"
        "## OUTPUT FORMAT — THIS IS CRITICAL\n\n"
        "Your ENTIRE output must be this table and nothing else. No prose before or after.\n\n"
        "name | title | email | phone | notes\n\n"
        "Rules:\n"
        "- Use — for unknown fields. Never leave blank.\n"
        "- Row 1 = the single best way to reach them. Put \"PREFERRED\" in notes.\n"
        "- Order remaining rows by likelihood of getting a response (best first).\n"
        "- Do NOT include rows for platforms where you confirmed they have no presence.\n"
        "- Do NOT include prose, headers, summaries. The table IS the output.\n"
        "- Include paywall rows at the bottom.\n"
        "- **CITATIONS ARE MANDATORY.** Every claim in the notes column MUST include the full URL "
        "where you found or confirmed that information. If you cannot provide a URL for a claim, "
        "say \"[no URL found]\"."
    )


CLEANUP_INSTRUCTIONS = (
    "You are a strict contact data filter. You will receive a contact lookup table in "
    "pipe-delimited format with columns: name | title | email | phone | notes.\n\n"
    "YOUR ONE AND ONLY JOB: Delete every row that does NOT have a real email address or a real phone number.\n\n"
    "RULES — FOLLOW THESE EXACTLY:\n"
    "- A row MUST have a real value in the \"email\" column OR the \"phone\" column to survive.\n"
    "- \"—\" is NOT a real value. Empty is NOT a real value. \"(behind paywall)\" is NOT real. \"(redacted)\" is NOT real.\n"
    "- Social media handles (Twitter, LinkedIn, Instagram, TikTok, Facebook, YouTube, Substack) are NOT email or phone. DELETE these rows.\n"
    "- PAYWALL rows have no real email or phone. DELETE them.\n"
    "- Booking agent rows with only a phone number and no email: KEEP (they have a real phone).\n"
    "- Gatekeeper rows with no email and no phone: DELETE.\n"
    "- \"DERIVED\" emails like andrew@forwardparty.com count as real emails. KEEP those rows.\n\n"
    "For each row you KEEP: visit the URLs in the notes column and verify they work. "
    "Add \"[verified]\" or \"[broken link]\" next to each URL.\n\n"
    "Return ONLY the filtered table with the header row. No prose before or after. No explanation. Just the table."
)


def get_record(record_id):
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    resp = http_requests.get(f"{AIRTABLE_URL}/{record_id}", headers=headers)
    resp.raise_for_status()
    return resp.json()


def update_record(record_id, content):
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"records": [{"id": record_id, "fields": {"Contact Info Raw": content}}]}
    resp = http_requests.patch(AIRTABLE_URL, headers=headers, json=payload)
    resp.raise_for_status()


def run_stage1(prompt):
    """Stage 1: Claude Opus with web search — contact lookup."""
    with anthropic_client.messages.stream(
        model="claude-opus-4-6",
        max_tokens=64000,
        thinking={"type": "enabled", "budget_tokens": 50000},
        tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 20}],
        messages=[{"role": "user", "content": prompt}],
    ) as stream:
        for event in stream:
            pass
        response = stream.get_final_message()

    content = ""
    thinking = ""
    for block in response.content:
        if block.type == "text":
            content += block.text
        elif block.type == "thinking":
            thinking += block.thinking

    return {
        "content": content,
        "model": response.model,
        "input_tokens": response.usage.input_tokens,
        "output_tokens": response.usage.output_tokens,
        "thinking_preview": thinking[:500] + ("..." if len(thinking) > 500 else ""),
    }


def extract_output_text(response_data):
    output = response_data.get("output", [])
    for item in reversed(output):
        if item.get("type") == "message":
            content = item.get("content", [])
            if content and content[0].get("text"):
                return content[0]["text"]
    return None


def run_stage2(stage1_text):
    """Stage 2: OpenAI o4-mini-deep-research — filter and verify contacts."""
    api_key = os.environ["OPENAI_API_KEY"]
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    # Start deep research in background mode
    resp = http_requests.post(
        "https://api.openai.com/v1/responses",
        headers=headers,
        json={
            "model": "o4-mini-deep-research",
            "input": stage1_text,
            "instructions": CLEANUP_INSTRUCTIONS,
            "background": True,
            "tools": [{"type": "web_search_preview"}],
        },
    )
    resp.raise_for_status()
    initial = resp.json()

    if initial.get("status") == "completed":
        return {
            "content": extract_output_text(initial) or stage1_text,
            "raw": initial,
        }

    # Poll until complete
    response_id = initial["id"]
    for _ in range(120):  # 10 minutes at 5s intervals
        poll = http_requests.get(
            f"https://api.openai.com/v1/responses/{response_id}",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        poll.raise_for_status()
        data = poll.json()

        if data["status"] == "completed":
            return {
                "content": extract_output_text(data) or stage1_text,
                "raw": data,
            }
        if data["status"] in ("failed", "cancelled"):
            raise Exception(f"Deep research {data['status']}: {json.dumps(data.get('error', {}))}")
        time.sleep(5)

    raise Exception("Deep research timed out after 10 minutes")


def process_record(record_id):
    try:
        record = get_record(record_id)
        fields = record["fields"]
        name = fields.get("Name", "")
        if not name:
            jobs[record_id]["status"] = "error"
            jobs[record_id]["error"] = "No name found"
            return

        reference_url = fields.get("Person Link", "")
        jobs[record_id]["name"] = name
        jobs[record_id]["status"] = "stage1"
        start = time.time()

        prompt = build_contact_prompt(name, reference_url)
        jobs[record_id]["prompt"] = prompt

        # Stage 1: Claude contact lookup
        stage1 = run_stage1(prompt)
        jobs[record_id]["stage1"] = stage1
        elapsed1 = time.time() - start
        jobs[record_id]["stage1_elapsed"] = f"{elapsed1:.0f}s"

        # Stage 2: o4-mini cleanup
        if stage1["content"]:
            jobs[record_id]["status"] = "stage2"
            try:
                stage2 = run_stage2(stage1["content"])
                jobs[record_id]["stage2"] = stage2
            except Exception as e:
                jobs[record_id]["stage2"] = {"content": "", "error": str(e)[:300]}

        elapsed = time.time() - start
        jobs[record_id]["elapsed"] = f"{elapsed:.0f}s"

        # Write to Airtable: prefer stage2 cleaned output, fall back to stage1
        stage2_result = jobs[record_id].get("stage2")
        if stage2_result and isinstance(stage2_result, dict) and stage2_result.get("content"):
            jobs[record_id]["status"] = "writing"
            update_record(record_id, stage2_result["content"])
        elif stage1["content"]:
            jobs[record_id]["status"] = "writing"
            update_record(record_id, stage1["content"])

        jobs[record_id]["status"] = "done"
        jobs[record_id]["finished"] = datetime.now().strftime("%H:%M:%S")

    except Exception as e:
        jobs[record_id]["status"] = "error"
        jobs[record_id]["error"] = str(e)[:300]


# --- Routes ---

@app.route("/run")
def run():
    record_id = request.args.get("record_id")
    if not record_id:
        return jsonify({"error": "Missing record_id"}), 400
    jobs[record_id] = {
        "name": "loading...",
        "status": "started",
        "started": datetime.now().strftime("%H:%M:%S"),
        "finished": "-",
        "elapsed": "-",
        "stage1_elapsed": "-",
        "error": "",
        "stage1": None,
        "stage2": None,
        "prompt": "",
    }
    threading.Thread(target=process_record, args=(record_id,)).start()
    return jsonify({"status": "started", "record_id": record_id})


@app.route("/api/output/<record_id>/<stage_key>")
def api_output(record_id, stage_key):
    job = jobs.get(record_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    result = job.get(stage_key)
    if not result or not isinstance(result, dict):
        return jsonify({"error": "No output"}), 404
    return jsonify({
        "model": result.get("model", "o4-mini-deep-research" if stage_key == "stage2" else "?"),
        "content": result.get("content", ""),
        "input_tokens": result.get("input_tokens", 0),
        "output_tokens": result.get("output_tokens", 0),
        "error": result.get("error", ""),
        "thinking_preview": result.get("thinking_preview", ""),
    })


@app.route("/log/<record_id>")
def log_view(record_id):
    job = jobs.get(record_id)
    if not job:
        return "Job not found", 404
    name = job.get("name", "?")
    status = job.get("status", "?")

    sections = ""
    for stage_key, label in [("stage1", "Stage 1 — Claude Opus (Contact Lookup)"), ("stage2", "Stage 2 — o4-mini (Cleanup)")]:
        result = job.get(stage_key)
        if result and isinstance(result, dict):
            content = html_mod.escape(result.get("content", "")) or "<em>No output</em>"
            error = result.get("error", "")
            model_id = result.get("model", "?")
            in_tok = result.get("input_tokens", 0)
            out_tok = result.get("output_tokens", 0)
            thinking = html_mod.escape(result.get("thinking_preview", ""))
            stats_line = f"Model: {model_id} | Input: {in_tok} | Output: {out_tok}"
            if error:
                stats_line += f" | Error: {error}"
            sections += f"""<h2 id="{stage_key}">{label}</h2>
<p style="color:#888;font-size:12px">{stats_line}</p>
{"<details><summary>Thinking preview</summary><pre>" + thinking + "</pre></details>" if thinking else ""}
<pre>{content}</pre>
"""
        elif result is None:
            sections += f"<h2 id=\"{stage_key}\">{label}</h2><p style='color:#888'>Pending...</p>\n"
        else:
            sections += f"<h2 id=\"{stage_key}\">{label}</h2><p style='color:red'>Failed</p>\n"

    prompt_text = html_mod.escape(job.get("prompt", ""))
    return f"""<!DOCTYPE html>
<html><head><title>Log: {name}</title>
<style>
body {{ font-family: monospace; max-width: 1000px; margin: 40px auto; padding: 0 20px; font-size: 13px; }}
h1, h2 {{ font-family: system-ui; }}
h1 {{ font-size: 1.2em; }}
h2 {{ font-size: 1em; margin-top: 30px; border-bottom: 1px solid #ddd; padding-bottom: 5px; }}
pre {{ background: #1e1e1e; color: #d4d4d4; padding: 20px; border-radius: 8px; overflow-x: auto; white-space: pre-wrap; word-wrap: break-word; }}
details {{ margin: 5px 0; }}
summary {{ cursor: pointer; font-family: system-ui; font-size: 12px; color: #666; }}
p {{ font-family: system-ui; }}
.footer {{ position: fixed; bottom: 0; left: 0; right: 0; background: #222; padding: 10px 20px; text-align: center; }}
.footer a {{ color: #4fc3f7; text-decoration: none; font-family: system-ui; font-size: 14px; }}
</style></head><body>
<h1><a href="/" style="text-decoration:none">&#8592;</a> {name} — {status} — {job.get('elapsed','-')}</h1>
{sections}
<h2>Prompt</h2>
<details><summary>Click to expand</summary>
<pre>{prompt_text}</pre>
</details>
<div style="height:50px"></div>
<div class="footer"><a href="/">Dashboard</a></div>
</body></html>"""


def _stage_cell(job, key, record_id):
    result = job.get(key)
    if result is None:
        return "<td style='color:#888'>—</td>"
    if isinstance(result, dict):
        error = result.get("error", "")
        if error:
            return f"<td style='color:red' title='{html_mod.escape(error)}'>err</td>"
        return f"<td><a href='#' onclick=\"showModal('{record_id}','{key}');return false\" style='color:#0a0'>view</a></td>"
    return "<td>?</td>"


def _build_rows():
    rows = ""
    for rid, j in sorted(jobs.items(), key=lambda x: x[1].get("started", ""), reverse=True):
        status = j["status"]
        color = {
            "started": "#888", "stage1": "#f90", "stage2": "#c0f", "writing": "#09f",
            "done": "#0a0", "error": "#f00",
        }.get(status, "#888")
        log_link = f'<a href="/log/{rid}">view</a>'
        rows += (
            f"<tr>"
            f"<td>{j.get('name','?')}</td>"
            f"<td style='color:{color};font-weight:bold'>{status}</td>"
            f"<td>{j.get('started','-')}</td>"
            f"<td>{j.get('finished','-')}</td>"
            f"<td>{j.get('elapsed','-')}</td>"
            f"{_stage_cell(j, 'stage1', rid)}"
            f"{_stage_cell(j, 'stage2', rid)}"
            f"<td>{log_link}</td>"
            f"<td style='color:red'>{j.get('error','')}</td>"
            f"</tr>"
        )
    if not rows:
        rows = "<tr><td colspan=9 style='text-align:center;color:#888'>No jobs yet</td></tr>"
    return rows


@app.route("/api/rows")
def api_rows():
    return _build_rows()


@app.route("/")
def index():
    rows = _build_rows()
    return f"""<!DOCTYPE html>
<html><head><title>Contact Lookup Server</title>
<style>
body {{ font-family: system-ui; max-width: 1100px; margin: 40px auto; padding: 0 20px; }}
h1 {{ font-size: 1.2em; }}
table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
th, td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid #ddd; }}
th {{ background: #f5f5f5; }}
a {{ color: #0066cc; }}
.modal-overlay {{ display:none; position:fixed; top:0; left:0; right:0; bottom:0; background:rgba(0,0,0,0.6); z-index:100; }}
.modal {{ position:fixed; top:5%; left:10%; right:10%; bottom:5%; background:#fff; border-radius:12px; z-index:101; display:flex; flex-direction:column; box-shadow:0 8px 32px rgba(0,0,0,0.3); }}
.modal-header {{ padding:16px 20px; border-bottom:1px solid #ddd; display:flex; justify-content:space-between; align-items:center; }}
.modal-header h2 {{ margin:0; font-size:1em; }}
.modal-header .close {{ cursor:pointer; font-size:1.5em; color:#888; border:none; background:none; }}
.modal-header .close:hover {{ color:#000; }}
.modal-stats {{ padding:8px 20px; color:#888; font-size:12px; border-bottom:1px solid #eee; }}
.modal-body {{ flex:1; overflow-y:auto; padding:20px; }}
.modal-body pre {{ background:#1e1e1e; color:#d4d4d4; padding:20px; border-radius:8px; white-space:pre-wrap; word-wrap:break-word; font-size:13px; margin:0; }}
</style></head><body>
<div class="modal-overlay" id="modalOverlay" onclick="closeModal()"></div>
<div class="modal" id="modal" style="display:none">
  <div class="modal-header">
    <h2 id="modalTitle">Loading...</h2>
    <button class="close" onclick="closeModal()">&times;</button>
  </div>
  <div class="modal-stats" id="modalStats"></div>
  <div class="modal-body"><pre id="modalContent">Loading...</pre></div>
</div>
<script>
var modalOpen=false;
setInterval(function(){{
  if(!modalOpen){{
    fetch('/api/rows').then(function(r){{return r.text()}}).then(function(html){{
      document.getElementById('jobRows').innerHTML=html;
    }});
  }}
}},5000);
function showModal(rid, key) {{
  modalOpen=true;
  document.getElementById('modalOverlay').style.display='block';
  document.getElementById('modal').style.display='flex';
  document.getElementById('modalTitle').textContent='Loading...';
  document.getElementById('modalStats').textContent='';
  document.getElementById('modalContent').textContent='Loading...';
  fetch('/api/output/'+rid+'/'+key)
    .then(r=>r.json())
    .then(d=>{{
      var labels={{stage1:'Stage 1 — Claude Opus',stage2:'Stage 2 — o4-mini Cleanup'}};
      document.getElementById('modalTitle').textContent=labels[key]||key;
      document.getElementById('modalStats').textContent='Model: '+d.model+' | Input: '+d.input_tokens+' | Output: '+d.output_tokens;
      document.getElementById('modalContent').textContent=d.content||d.error||'No output';
    }})
    .catch(e=>{{
      document.getElementById('modalTitle').textContent='Error';
      document.getElementById('modalContent').textContent=e.toString();
    }});
}}
function closeModal() {{
  modalOpen=false;
  document.getElementById('modalOverlay').style.display='none';
  document.getElementById('modal').style.display='none';
}}
document.addEventListener('keydown',function(e){{ if(e.key==='Escape') closeModal(); }});
</script>
<h1>Contact Lookup Server — 2-Stage Pipeline</h1>
<p style="color:#888;font-size:0.8em;margin-top:-10px">commit: {os.environ.get('RENDER_GIT_COMMIT','local')[:7]}</p>
<table>
<tr><th>Name</th><th>Status</th><th>Started</th><th>Finished</th><th>Time</th><th>Stage 1</th><th>Stage 2</th><th>Log</th><th>Error</th></tr>
<tbody id="jobRows">{rows}</tbody>
</table>
</body></html>"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8787)))
