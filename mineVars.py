import requests
import re
import time
import argparse
from requests.adapters import HTTPAdapter, Retry

# ==============================
# Configuration
# ==============================
REQUEST_DELAY = 0.34  # ~3 requests/sec
USER_AGENT = {"User-Agent": "uniprot-fetcher/1.0 (+https://example.org)"}
OUTPUT_FILE = "uniprot_Bacteroides816_variants.tsv"

BASE_URL = "https://rest.uniprot.org/uniprotkb/search"
SIZE = 500  # UniProt max page size

# good idea to split fields to stay under URL length limits 
# FIELDS = (
#    "accession,id,gene_names,gene_primary,gene_synonym,gene_oln,gene_orf,"
#    "organism_name,organism_id,protein_name,xref_proteomes,lineage,lineage_ids,"
#    "virus_hosts,ft_variant,cc_function,cc_pathway,cc_disease,lit_pubmed_id"
#)
FIELDS = (
        "accession,id,gene_names,gene_primary,gene_synonym,gene_oln,gene_orf,"
        "organism_name,organism_id,protein_name,xref_proteomes,lineage,lineage_ids,"
        "virus_hosts,cc_alternative_products,ft_var_seq,fragment,length,mass,cc_mass_spectrometry,"
        "ft_variant,xref_biomuta,xref_dbsnp,xref_dmdm,ft_non_cons,ft_non_std,ft_non_ter,"
        "cc_polymorphism,cc_rna_editing,sequence,cc_sequence_caution,ft_conflict,ft_unsure,"
        "sequence_version,absorption,ft_act_site,cc_activity_regulation,ft_binding,cc_catalytic_activity,"
        "cc_cofactor,ft_dna_bind,ec,cc_function,kinetics,cc_pathway,ph_dependence,redox_potential,rhea,"
        "ft_site,temp_dependence,annotation_score,cc_caution,comment_count,feature_count,keywordid,"
        "keyword,cc_miscellaneous,protein_existence,reviewed,tools,uniparc_id,cc_interaction,cc_subunit,"
        "cc_developmental_stage,cc_induction,cc_tissue_specificity,go_p,go_c,go,go_f,go_id,cc_allergen,"
        "cc_biotechnology,cc_disruption_phenotype,cc_disease,ft_mutagen,cc_pharmaceutical,cc_toxic_dose,"
        "ft_intramem,cc_subcellular_location,ft_topo_dom,ft_transmem,ft_chain,ft_crosslnk,ft_disulfid,"
        "ft_carbohyd,ft_init_met,ft_lipid,ft_mod_res,ft_peptide,cc_ptm,ft_propep,ft_signal,ft_transit,"
        "structure_3d,ft_strand,ft_helix,ft_turn,lit_pubmed_id,date_created,date_modified,date_sequence_modified,"
        "version,ft_coiled,ft_compbias,cc_domain,ft_domain,ft_motif,protein_families,ft_region,ft_repeat,ft_zn_fing"
        )

# ==============================
# HTTP Session Setup (with retry)
# ==============================
re_next_link = re.compile(r'<(.+)>; rel="next"')
retries = Retry(
    total=5,
    backoff_factor=0.25,
    status_forcelist=[500, 502, 503, 504],
)
session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=retries))
session.headers.update(USER_AGENT)


# ==============================
# Helper Functions
# ==============================
def info(msg: str):
    print(f"[INFO] {msg}")


def throttle_request():
    """Delay between requests to comply with UniProt limits"""
    time.sleep(REQUEST_DELAY)


def get_next_link(headers):
    """Extract next page URL from 'Link' header"""
    if "Link" in headers:
        match = re_next_link.match(headers["Link"])
        if match:
            return match.group(1)
    return None


def get_batch(batch_url):
    """Generator yielding each paginated response"""
    while batch_url:
        throttle_request()
        response = session.get(batch_url)
        response.raise_for_status()
        total = response.headers.get("x-total-results", "unknown")
        yield response, total
        batch_url = get_next_link(response.headers)


# ==============================
# Build Initial URL
# ==============================
def build_query_url(tax_id, reviewed):
    query = f"reviewed:{reviewed} AND taxonomy_id:{tax_id}"
    params = {
        "query": query,
        "fields": FIELDS,
        "format": "tsv",
        "size": SIZE,
    }
    return BASE_URL + "?" + "&".join(f"{k}={requests.utils.quote(str(v))}" for k, v in params.items())


# ==============================
# Main Download Loop
# ==============================
def fetch_all_records(tax_id, reviewed, output_file):
    url = build_query_url(tax_id, reviewed)
    total = 0
    header_written = False

    with open(output_file, "w", encoding="utf-8") as out:
        for batch, total_records in get_batch(url):
            lines = batch.text.splitlines()
            if not lines:
                info("Empty response batch — stopping.")
                break

            # Write header once
            if not header_written:
                out.write(lines[0] + "\n")
                lines = lines[1:]
                header_written = True

            for line in lines:
                out.write(line + "\n")
                total += 1

            info(f"Fetched {total} / {total_records} records so far.")

    info(f"Completed. Total downloaded: {total}")


# ==============================
# CLI Interface
# ==============================
def main():
    parser = argparse.ArgumentParser(description="Download UniProtKB entries for a given taxonomic ID.")
    parser.add_argument(
        "-t", "--taxid",
        type=int,
        required=True,
        help="NCBI taxonomy ID (e.g., 816 for Bacteroides genus)"
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        default="uniprot_output.tsv",
        help="Output TSV file path"
    )
    parser.add_argument(
        "-x", "--reviewed",
        type=str,
        default="true",
        help="Output TSV file path"
    )

    args = parser.parse_args()
    info(f"Starting UniProtKB download for taxid={args.taxid} -> {args.output}")
    fetch_all_records(args.taxid, args.reviewed, args.output)


if __name__ == "__main__":
    main()

#if __name__ == "__main__":
#    info("Starting UniProtKB download for Bacteroides (taxonomy_id:816)...")
#    fetch_all_records()

