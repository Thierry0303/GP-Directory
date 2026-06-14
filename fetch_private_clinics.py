def paginate_london(key):
    print("Paginating CQC /locations (Independent Providers only) …")
    candidates = []
    page = 1
    per_page = 100  # CQC API stable maximum per page
    
    while True:
        # Using 'providerType' which is natively supported by the /locations endpoint
        params = {
            "page": page, 
            "perPage": per_page, 
            "providerType": "IndependentProvider"
        }
        data = cqc_get("/locations", params, key)
        if not data: break
        items = data.get("locations", []) or []
        if not items: break
        
        for loc in items:
            if loc.get("deregistrationDate"): continue
            pc = loc.get("postalCode") or ""
            if not is_london(pc): continue
            
            name = loc.get("locationName") or loc.get("name") or ""
            if DROP_NAME_RE.search(name): continue
            candidates.append(loc)
            
        total_pages = data.get("totalPages", 1)
        if page % 20 == 0:
            print(f"  page {page}/{total_pages} — current candidates: {len(candidates)}")
        if page >= total_pages: break
        page += 1
        time.sleep(0.1)
        
    print(f"\nFound {len(candidates)} targeted London independent provider candidates.\n")
    return candidates
