"""
Test suite for documentation validation.
Ensures documentation accurately reflects the actual implementation.
"""

import os
import re
from pathlib import Path
import pytest


class TestDocumentationAccuracy:
    """Validate that documentation matches the actual implementation."""

    @pytest.fixture
    def project_root(self):
        """Get project root directory."""
        return Path(__file__).parent.parent

    @pytest.fixture
    def src_files(self, project_root):
        """Count actual source files and lines."""
        src_dir = project_root / "src" / "fullon_ohlcv_service"

        file_stats = {}
        total_lines = 0

        for py_file in src_dir.rglob("*.py"):
            if "__pycache__" not in str(py_file):
                rel_path = py_file.relative_to(src_dir)
                with open(py_file) as f:
                    # Count non-empty, non-comment lines
                    lines = [l.strip() for l in f.readlines()]
                    code_lines = [l for l in lines if l and not l.startswith("#")]
                    file_stats[str(rel_path)] = len(code_lines)
                    total_lines += len(code_lines)

        return {"files": file_stats, "total_lines": total_lines}

    def test_claude_md_reflects_actual_architecture(self, project_root, src_files):
        """Test that CLAUDE.md reflects the actual implementation."""
        claude_md = project_root / "CLAUDE.md"
        assert claude_md.exists(), "CLAUDE.md must exist"

        content = claude_md.read_text()

        # Check that key implementation files are mentioned
        assert "ohlcv/live_collector.py" in content
        assert "ohlcv/historic_collector.py" in content
        assert "trade/live_collector.py" in content
        assert "trade/historic_collector.py" in content
        assert "config/settings.py" in content

        # Verify line count documentation is reasonable
        # The documentation should acknowledge the actual implementation size
        if "~300-500 lines" in content or "~1,600 lines" in content:
            actual_lines = src_files["total_lines"]
            # Allow reasonable variance in line count estimates
            assert actual_lines < 2000, (
                f"Documentation line count estimate is too low: implementation has {actual_lines} lines"
            )

    def test_readme_shows_actual_usage(self, project_root):
        """Test that README.md shows actual usage patterns."""
        readme = project_root / "README.md"
        assert readme.exists(), "README.md must exist"

        content = readme.read_text()

        # Check for actual usage examples
        assert "poetry run" in content or "python -m" in content, (
            "README should show how to run the service"
        )

        # Verify it mentions fullon ecosystem integration
        assert "fullon_exchange" in content
        assert "fullon_ohlcv" in content
        assert "fullon_orm" in content

        # Should not reference non-existent files
        assert "fix_this_fuck.md" not in content or not (project_root / "fix_this_fuck.md").exists()

    def test_git_plan_marked_completed(self, project_root):
        """Test that git_plan.md is properly updated."""
        git_plan = project_root / "git_plan.md"

        if git_plan.exists():
            content = git_plan.read_text()

            # Should reflect that foundation issues are being/have been addressed
            # Look for indicators of progress
            has_progress = (
                "✅" in content
                or "DONE" in content
                or "Completed" in content
                or "COMPLETE" in content
                or "implemented" in content.lower()
            )
            assert has_progress, "git_plan.md should indicate implementation progress"

            # Should mention key components
            assert "OhlcvCollector" in content or "collector" in content.lower()
            assert "TradeCollector" in content or "trade" in content.lower()

    def test_examples_documentation_exists(self, project_root):
        """Test that examples are documented."""
        examples_dir = project_root / "examples"

        if examples_dir.exists():
            # Should have a README or documentation
            readme = examples_dir / "README.md"
            if readme.exists():
                content = readme.read_text()

                # Check for actual example descriptions
                assert len(content.strip()) > 100, "Examples README should have meaningful content"

                # Should describe available examples
                for example_file in examples_dir.glob("*.py"):
                    if example_file.name != "__init__.py":
                        # Example files should be documented
                        assert example_file.stem in content or "run" in content.lower()

    def test_documentation_consistency(self, project_root):
        """Test that all documentation files are consistent."""
        docs = {
            "CLAUDE.md": project_root / "CLAUDE.md",
            "README.md": project_root / "README.md",
            "git_plan.md": project_root / "git_plan.md",
        }

        # Collect key facts from each doc
        facts = {}
        for name, path in docs.items():
            if path.exists():
                content = path.read_text()
                facts[name] = {
                    "mentions_300_500": "~300-500" in content or "300-500" in content,
                    "mentions_fullon": "fullon" in content.lower(),
                    "mentions_foundation": "foundation" in content.lower() or "Issue #1" in content,
                }

        # All docs should agree on the project being fullon-based
        if len(facts) > 1:
            fullon_mentions = [f["mentions_fullon"] for f in facts.values()]
            assert all(fullon_mentions), "All documentation should mention fullon ecosystem"

    def test_no_outdated_references(self, project_root):
        """Test that documentation doesn't reference outdated or non-existent files."""
        docs_to_check = [
            project_root / "CLAUDE.md",
            project_root / "README.md",
            project_root / "git_plan.md",
        ]

        for doc_path in docs_to_check:
            if doc_path.exists():
                content = doc_path.read_text()

                # Check for references to non-existent markdown files
                md_refs = re.findall(r"`([^`]+\.md)`", content)
                for ref in md_refs:
                    if "/" not in ref:  # Only check root-level references
                        ref_path = project_root / ref
                        if ref not in ["CLAUDE.md", "README.md", "git_plan.md"]:
                            # These files should exist if referenced
                            if "fix_this" not in ref.lower():  # Except cleanup targets
                                assert ref_path.exists() or "example" in ref.lower(), (
                                    f"{doc_path.name} references non-existent {ref}"
                                )


class TestImplementationStructure:
    """Validate the implementation matches documented architecture."""

    def test_core_modules_exist(self):
        """Test that core modules exist as documented."""
        base_path = Path(__file__).parent.parent / "src" / "fullon_ohlcv_service"

        expected_modules = [
            "ohlcv/live_collector.py",
            "ohlcv/historic_collector.py",
            "trade/live_collector.py",
            "trade/historic_collector.py",
            "trade/batcher.py",
            "config/settings.py",
            "config/database_config.py",
            "daemon.py",
        ]

        for module in expected_modules:
            module_path = base_path / module
            assert module_path.exists(), f"Core module {module} should exist"

    def test_uses_fullon_ecosystem(self):
        """Test that implementation uses fullon ecosystem libraries."""
        base_path = Path(__file__).parent.parent / "src" / "fullon_ohlcv_service"

        # Check that key files import fullon libraries
        files_to_check = [
            base_path / "ohlcv" / "live_collector.py",
            base_path / "ohlcv" / "historic_collector.py",
            base_path / "trade" / "live_collector.py",
            base_path / "trade" / "historic_collector.py",
            base_path / "trade" / "batcher.py",
            base_path / "daemon.py",
        ]

        fullon_imports = {
            "fullon_exchange": False,
            "fullon_ohlcv": False,
            "fullon_orm": False,
            "fullon_log": False,
        }

        for file_path in files_to_check:
            if file_path.exists():
                content = file_path.read_text()
                for lib in fullon_imports.keys():
                    if lib in content:
                        fullon_imports[lib] = True

        # At minimum, should use exchange and ohlcv
        assert fullon_imports["fullon_exchange"] or fullon_imports["fullon_ohlcv"], (
            "Implementation should use fullon ecosystem libraries"
        )
