"""
Vercel Serverless Function — Odoo Email Blast Dashboard Data
=============================================================
Fetches live data from Odoo for the email blast effectiveness dashboard.
Credentials are stored as Vercel environment variables.
Response is cached at the CDN edge for 5 minutes.
"""
import os
import json
import xmlrpc.client
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from http.server import BaseHTTPRequestHandler


BATCH_SIZE = 200


def connect():
    url = os.environ['ODOO_URL']
    db = os.environ['ODOO_DB']
    user = os.environ['ODOO_USER']
    api_key = os.environ['ODOO_API_KEY']
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
    uid = common.authenticate(db, user, api_key, {})
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
    return uid, models, db, api_key


def call(models, uid, db, api_key, model, method, args, kwargs=None):
    return models.execute_kw(db, uid, api_key, model, method, args, kwargs or {})


def parse_dt(val):
    if not val or val == 'False':
        return None
    if isinstance(val, str):
        return datetime.strptime(val[:19], '%Y-%m-%d %H:%M:%S')
    return val


def collect_mailing(models, uid, db, api_key, mailing, trace_fk, trace_fields):
    mid = mailing['id']
    sent_date = mailing.get('sent_date')
    blast_date = parse_dt(sent_date)

    # Traces
    traces = call(models, uid, db, api_key, 'mailing.trace', 'search_read',
                  [[(trace_fk, '=', mid)]], {'fields': trace_fields, 'limit': 5000}) or []

    # Lead IDs
    lead_ids = list(set(t['res_id'] for t in traces
                        if t.get('model') == 'crm.lead' and t.get('res_id')))
    if not lead_ids:
        pids = list(set(t['res_id'] for t in traces
                        if t.get('model') == 'res.partner' and t.get('res_id')))
        if pids:
            lead_ids = call(models, uid, db, api_key, 'crm.lead', 'search',
                            [[('partner_id', 'in', pids)]]) or []
    if not lead_ids:
        return None

    # Read leads
    all_leads = []
    for i in range(0, len(lead_ids), BATCH_SIZE):
        batch = call(models, uid, db, api_key, 'crm.lead', 'read',
                     [lead_ids[i:i + BATCH_SIZE]],
                     {'fields': ['id', 'name', 'email_from', 'partner_id',
                                 'stage_id', 'type', 'active', 'probability',
                                 'create_date', 'date_closed',
                                 'date_last_stage_update']})
        if batch:
            all_leads.extend(batch)
    lead_map = {l['id']: l for l in all_leads}

    # Stage changes
    stage_changes = []
    for i in range(0, len(lead_ids), BATCH_SIZE):
        chunk = lead_ids[i:i + BATCH_SIZE]
        msg_ids = call(models, uid, db, api_key, 'mail.message', 'search',
                       [[('model', '=', 'crm.lead'),
                         ('res_id', 'in', chunk),
                         ('tracking_value_ids', '!=', False)]],
                       {'limit': 10000})
        if not msg_ids:
            continue
        tvs = call(models, uid, db, api_key, 'mail.tracking.value', 'search_read',
                   [[('mail_message_id', 'in', msg_ids),
                     ('field_id.name', '=', 'stage_id')]],
                   {'fields': ['mail_message_id', 'old_value_char',
                               'new_value_char', 'create_date'],
                    'order': 'create_date asc', 'limit': 10000})
        if not tvs:
            continue
        uniq = list(set(
            tv['mail_message_id'][0] if isinstance(tv['mail_message_id'], list)
            else tv['mail_message_id'] for tv in tvs))
        msgs = call(models, uid, db, api_key, 'mail.message', 'read',
                    [uniq], {'fields': ['id', 'res_id', 'date']})
        m2l = {m['id']: m['res_id'] for m in (msgs or [])}
        m2d = {m['id']: m['date'] for m in (msgs or [])}
        for tv in tvs:
            msgid = (tv['mail_message_id'][0] if isinstance(tv['mail_message_id'], list)
                     else tv['mail_message_id'])
            tv['lead_id'] = m2l.get(msgid)
            tv['message_date'] = m2d.get(msgid)
            stage_changes.append(tv)

    # Classify
    cbl = defaultdict(list)
    for sc in stage_changes:
        if sc.get('lead_id'):
            cbl[sc['lead_id']].append(sc)

    moved = []
    for lid in lead_ids:
        lead = lead_map.get(lid, {})
        changes = cbl.get(lid, [])
        post = []
        for c in changes:
            cdt = parse_dt(c.get('message_date') or c.get('create_date'))
            if blast_date and cdt and cdt > blast_date:
                post.append(c)
            elif not blast_date:
                post.append(c)
        if post:
            trace = next((t for t in traces if t.get('res_id') == lid), None)
            moved.append({
                'lead_id': lid,
                'name': lead.get('name', ''),
                'email': lead.get('email_from', ''),
                'current_stage': (lead.get('stage_id', [False, ''])[1]
                                  if isinstance(lead.get('stage_id'), list) else ''),
                'opened': bool(trace and trace.get('open_datetime')) if trace else False,
                'clicked': bool(trace and trace.get('links_click_datetime')) if trace else False,
                'changes': [{
                    'date': str(c.get('message_date') or c.get('create_date', '')),
                    'from': c.get('old_value_char', ''),
                    'to': c.get('new_value_char', '')
                } for c in post]
            })

    trans = defaultdict(int)
    for m in moved:
        for c in m['changes']:
            trans[f"{c['from']} -> {c['to']}"] += 1

    stage_dist = defaultdict(int)
    for l in all_leads:
        sn = (l.get('stage_id', [False, 'Unknown'])[1]
              if isinstance(l.get('stage_id'), list) else 'Unknown')
        stage_dist[sn] += 1

    total_opened = sum(1 for t in traces if t.get('open_datetime'))
    total_clicked = sum(1 for t in traces if t.get('links_click_datetime'))

    return {
        'mailing_id': mid,
        'subject': mailing.get('subject', ''),
        'sent_date': str(sent_date or ''),
        'state': mailing.get('state', ''),
        'stats': {
            'sent': mailing.get('sent', 0),
            'delivered': mailing.get('delivered', 0),
            'opened': mailing.get('opened', 0),
            'clicked': mailing.get('clicked', 0),
            'replied': mailing.get('replied', 0),
            'bounced': mailing.get('bounced', 0),
        },
        'total_leads': len(lead_ids),
        'total_traces': len(traces),
        'total_opened': total_opened,
        'total_clicked': total_clicked,
        'leads_moved': len(moved),
        'stage_transitions': dict(trans),
        'stage_distribution': dict(stage_dist),
        'moved_leads': moved,
    }


def fetch_all():
    uid, models, db, api_key = connect()

    # Find sent mailings
    mailings = call(models, uid, db, api_key, 'mailing.mailing', 'search_read',
                    [[('subject', 'ilike', 'lowest prices'), ('state', '=', 'done')]],
                    {'fields': ['id', 'subject', 'state', 'sent_date',
                                'mailing_model_id', 'mailing_domain', 'campaign_id',
                                'sent', 'delivered', 'opened', 'clicked',
                                'replied', 'bounced'],
                     'order': 'id desc'})
    if not mailings:
        return {'error': 'No sent mailings found', 'data': []}

    # Discover trace fields
    info = call(models, uid, db, api_key, 'mailing.trace', 'fields_get', [],
                {'attributes': ['string', 'type']})
    names = list((info or {}).keys())
    trace_fk = next((c for c in ['mass_mailing_id', 'mailing_id'] if c in names), None)
    trace_fields = ['id']
    for f in ['model', 'res_id', 'sent_datetime', 'open_datetime',
              'reply_datetime', 'links_click_datetime', 'trace_status',
              'failure_type', 'email']:
        if f in names:
            trace_fields.append(f)

    if not trace_fk:
        return {'error': 'Cannot find mailing trace FK field', 'data': []}

    results = []
    for m in mailings:
        data = collect_mailing(models, uid, db, api_key, m, trace_fk, trace_fields)
        if data:
            results.append(data)

    return {
        'generated_at': datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M GMT+8'),
        'data': results
    }


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            result = fetch_all()
            body = json.dumps(result, default=str).encode('utf-8')
            self.send_response(200)
        except Exception as e:
            body = json.dumps({'error': str(e), 'data': []}).encode('utf-8')
            self.send_response(500)

        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        # Cache at CDN edge for 5 min, serve stale up to 10 min while revalidating
        self.send_header('Cache-Control', 's-maxage=300, stale-while-revalidate=600')
        self.end_headers()
        self.wfile.write(body)
