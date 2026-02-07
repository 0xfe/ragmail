#!/usr/bin/env python3
"""
MBOX Sampler
============
Extracts a sample of emails from an MBOX file for testing.
Samples from different parts of the file to get a representative distribution.

Usage:
    python sample-mbox.py input.mbox --lines 50000 -o sample.mbox
    python sample-mbox.py input.mbox --emails 1000 -o sample.mbox
"""

import os
import sys
import argparse
import random

from ragmail.common.terminal import Colors, Glyphs, format_bytes


def sample_by_lines(input_path: str, output_path: str, num_lines: int):
    """Extract first N lines from mbox file."""
    print(f"\n{Colors.CYAN}Sampling {num_lines:,} lines from {input_path}{Colors.RESET}")

    with open(input_path, 'rb') as fin, open(output_path, 'wb') as fout:
        for i, line in enumerate(fin):
            if i >= num_lines:
                break
            fout.write(line)

            if i > 0 and i % 10000 == 0:
                sys.stdout.write(f"\r  Lines written: {i:,}")
                sys.stdout.flush()

    output_size = os.path.getsize(output_path)
    print(f"\r{Colors.GREEN}{Glyphs.CHECK} Wrote {num_lines:,} lines ({format_bytes(output_size)}){Colors.RESET}")


def sample_by_emails(input_path: str, output_path: str, num_emails: int, random_sample: bool = False):
    """Extract N complete emails from mbox file."""
    print(f"\n{Colors.CYAN}Sampling {num_emails:,} emails from {input_path}{Colors.RESET}")

    file_size = os.path.getsize(input_path)

    # First pass: count emails and find their positions
    if random_sample:
        print("  Scanning file for email positions...")
        positions = []
        with open(input_path, 'rb') as f:
            pos = 0
            for line in f:
                if line.startswith(b'From ') and b'@' in line:
                    positions.append(pos)
                pos += len(line)

        print(f"  Found {len(positions):,} emails")

        # Randomly select positions
        if len(positions) > num_emails:
            selected = sorted(random.sample(positions, num_emails))
        else:
            selected = positions
            num_emails = len(positions)

        # Extract selected emails
        print(f"  Extracting {num_emails:,} random emails...")
        with open(input_path, 'rb') as fin, open(output_path, 'wb') as fout:
            selected_set = set(selected)
            current_email = []
            in_selected = False
            pos = 0

            for line in fin:
                if line.startswith(b'From ') and b'@' in line:
                    # Write previous email if it was selected
                    if in_selected and current_email:
                        fout.writelines(current_email)

                    current_email = [line]
                    in_selected = pos in selected_set
                else:
                    current_email.append(line)

                pos += len(line)

            # Don't forget last email
            if in_selected and current_email:
                fout.writelines(current_email)
    else:
        # Just take first N emails
        email_count = 0
        with open(input_path, 'rb') as fin, open(output_path, 'wb') as fout:
            current_email = []

            for line in fin:
                if line.startswith(b'From ') and b'@' in line:
                    # Write previous email
                    if current_email:
                        fout.writelines(current_email)
                        email_count += 1

                        if email_count % 100 == 0:
                            sys.stdout.write(f"\r  Emails written: {email_count:,}")
                            sys.stdout.flush()

                        if email_count >= num_emails:
                            break

                    current_email = [line]
                else:
                    current_email.append(line)

    output_size = os.path.getsize(output_path)
    print(f"\r{Colors.GREEN}{Glyphs.CHECK} Wrote {num_emails:,} emails ({format_bytes(output_size)}){Colors.RESET}")


def sample_distributed(input_paths: list, output_path: str, emails_per_file: int):
    """Sample emails from multiple mbox files for distributed testing."""
    print(f"\n{Colors.CYAN}{Colors.BOLD}Distributed Sampling{Colors.RESET}")
    print(f"Files: {len(input_paths)}")
    print(f"Emails per file: {emails_per_file:,}")
    print()

    with open(output_path, 'wb') as fout:
        total_emails = 0

        for input_path in input_paths:
            if not os.path.exists(input_path):
                print(f"  {Colors.YELLOW}Skipping {input_path} (not found){Colors.RESET}")
                continue

            year = os.path.basename(input_path).replace('gmail-', '').replace('.mbox', '')
            print(f"  {Glyphs.FOLDER} {year}: ", end='', flush=True)

            email_count = 0
            current_email = []

            with open(input_path, 'rb') as fin:
                for line in fin:
                    if line.startswith(b'From ') and b'@' in line:
                        if current_email:
                            fout.writelines(current_email)
                            email_count += 1
                            total_emails += 1

                            if email_count >= emails_per_file:
                                break

                        current_email = [line]
                    else:
                        current_email.append(line)

            print(f"{Colors.GREEN}{email_count:,} emails{Colors.RESET}")

    output_size = os.path.getsize(output_path)
    print(f"\n{Colors.GREEN}{Glyphs.CHECK} Total: {total_emails:,} emails ({format_bytes(output_size)}){Colors.RESET}")
    print(f"Output: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Sample emails from MBOX files for testing.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Sample first 50000 lines
  %(prog)s input.mbox --lines 50000 -o sample.mbox

  # Sample first 1000 emails
  %(prog)s input.mbox --emails 1000 -o sample.mbox

  # Sample from multiple years (distributed testing)
  %(prog)s private/gmail-*.mbox --distributed --emails-per-file 200 -o test-sample.mbox
        """
    )

    parser.add_argument('input', nargs='+', help='Input MBOX file(s)')
    parser.add_argument('-o', '--output', required=True, help='Output sample file')
    parser.add_argument('--lines', type=int, help='Number of lines to sample')
    parser.add_argument('--emails', type=int, help='Number of emails to sample')
    parser.add_argument('--distributed', action='store_true',
                        help='Sample from multiple files (use with --emails-per-file)')
    parser.add_argument('--emails-per-file', type=int, default=100,
                        help='Emails to sample per file in distributed mode')
    parser.add_argument('--random', action='store_true',
                        help='Random sample instead of sequential')

    args = parser.parse_args()

    if args.distributed:
        sample_distributed(args.input, args.output, args.emails_per_file)
    elif args.lines:
        if len(args.input) > 1:
            print(f"{Colors.RED}Error: --lines only works with single input file{Colors.RESET}")
            sys.exit(1)
        sample_by_lines(args.input[0], args.output, args.lines)
    elif args.emails:
        if len(args.input) > 1:
            print(f"{Colors.RED}Error: --emails only works with single input file (use --distributed){Colors.RESET}")
            sys.exit(1)
        sample_by_emails(args.input[0], args.output, args.emails, args.random)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
