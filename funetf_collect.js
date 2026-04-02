async (code) => {
    const DLY = 3000;
    const API = 'https://www.funetf.co.kr/api/public/product/view';
    const sleep = ms => new Promise(r => setTimeout(r, ms));
    
    let result = {
        code: code,
        volatility_3y: null, sharpe_3y: null, beta_3y: null,
        jensen_alpha_3y: null, info_ratio_3y: null,
        pct_rank_vol: null, pct_rank_sharpe: null, pct_rank_alpha: null,
        fund_feature: '', fund_strategy: ''
    };
    
    try {
        let pageResp = await fetch('/product/fund/view/' + code, {credentials: 'include'});
        let html = await pageResp.text();
        
        let p = {fundCd: code, gijunYmd: '20260323', wGijunYmd: '20260323'};
        
        function find(name) {
            let patterns = [
                name + ":'", name + ':"', name + "='", name + '="',
                name + ": '", name + ': "', name + "= '", name + '= "',
                name + ":'", name + ':"'
            ];
            for (let pat of patterns) {
                let idx = html.indexOf(pat);
                if (idx >= 0) {
                    let start = idx + pat.length;
                    let end = start;
                    while (end < html.length && end < start + 50) {
                        let ch = html[end];
                        if (ch === "'" || ch === '"' || ch === ',' || ch === '}' || ch === '&' || ch === ' ') break;
                        end++;
                    }
                    let val = html.substring(start, end).trim();
                    if (val.length > 0 && val.length < 40) return val;
                }
            }
            return null;
        }
        
        ['seoljYmd','repFundCd','ltypeCd','stypeCd','zeroinTypeLcd','zeroinTypeCd',
         'mketDvsn','gijunYmdNy','pfGijunYmd','pfGijunYmdBf12','pfGijunYmdInfo',
         'pfGijunYmdBf12Info','usdGijunYmd','spGijunYmd','_csrf','repFid','fid'].forEach(function(f) {
            let v = find(f);
            if (v) p[f] = v;
        });
        
        p.usdYn = 'N';
        p.roleGroupType = 'ANONYMOUS';
        p.roleType = 'ROLE_ANONYMOUS';
        
        let qs = function(obj) {
            let parts = [];
            for (let k in obj) { if (obj[k]) parts.push(k + '=' + encodeURIComponent(obj[k])); }
            return parts.join('&');
        };
        
        await sleep(DLY);
        let rp = Object.assign({}, p, {schRiskTerm: '36', schCtenDvsn: 'MK_VIEW'});
        let riskResp = await fetch(API + '/riskanalysis?' + qs(rp), {
            credentials: 'include',
            headers: {'Accept': 'application/json', 'X-Requested-With': 'XMLHttpRequest'}
        });
        
        if (riskResp.ok) {
            let risk = await riskResp.json();
            if (risk && risk.length >= 2 && risk[0]) {
                result.volatility_3y = risk[0].yyDev || null;
                result.sharpe_3y = risk[0].sharp || null;
                result.beta_3y = risk[0].betaMkt || null;
                result.jensen_alpha_3y = risk[0].alphaMkt || null;
                result.info_ratio_3y = risk[0].ir || null;
                if (risk[1]) {
                    result.pct_rank_vol = risk[1].yyDev || null;
                    result.pct_rank_sharpe = risk[1].sharp || null;
                    result.pct_rank_alpha = risk[1].alphaMkt || null;
                }
            }
        }
        
        await sleep(DLY);
        let descResp = await fetch(API + '/zeroindiscription?' + qs(p), {
            credentials: 'include',
            headers: {'Accept': 'application/json', 'X-Requested-With': 'XMLHttpRequest'}
        });
        
        if (descResp.ok) {
            let desc = await descResp.json();
            if (desc && desc.discription2 && desc.discription2.length > 0) {
                result.fund_feature = desc.discription2[0].discription3 || '';
                result.fund_strategy = desc.discription2[0].discription4 || '';
            }
        }
        
    } catch(e) {
        result._error = e.message;
    }
    
    return result;
}
