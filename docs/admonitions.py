"""Custom "Example" admonition and verbatim "Output" block."""

from docutils import nodes
from docutils.parsers.rst import Directive
from docutils.parsers.rst.directives.admonitions import BaseAdmonition
from sphinx.application import Sphinx
from sphinx.util.typing import ExtensionMetadata


class ExampleDirective(BaseAdmonition):
    """Renders as "Example" alone, or "Example: <argument>" when a title is given."""

    required_arguments = 0
    optional_arguments = 1
    final_argument_whitespace = True
    node_class = nodes.admonition

    def run(self) -> list[nodes.Node]:
        """Build an admonition node with a label/subtitle title, then parse its body."""
        self.options["classes"] = ["example-block"]
        self.assert_has_content()
        text = "\n".join(self.content)
        admonition_node = nodes.admonition(text, **self.options)
        self.add_name(admonition_node)
        admonition_node.source, admonition_node.line = self.state_machine.get_source_and_line(
            self.lineno
        )

        label_text = "Example:" if self.arguments else "Example"
        title = nodes.container("", classes=["admonition-title"])
        title += nodes.paragraph("", "", nodes.Text(label_text), classes=["admonition-title-label"])

        messages: list[nodes.system_message] = []
        if self.arguments:
            subtitle_nodes, messages = self.state.inline_text(self.arguments[0], self.lineno)
            title += nodes.paragraph("", "", *subtitle_nodes, classes=["admonition-title-subtitle"])

        admonition_node += title
        admonition_node += messages

        self.state.nested_parse(self.content, self.content_offset, admonition_node)
        return [admonition_node]


class OutputDirective(Directive):
    """Renders its content verbatim as plain preformatted text."""

    has_content = True

    def run(self) -> list[nodes.Node]:
        """Build a plain, unhighlighted "example-output" node from raw content lines."""
        self.assert_has_content()
        text = "\n".join(self.content)
        container = nodes.container("", classes=["example-output"])
        container += nodes.paragraph(text, "", nodes.Text(text))
        return [container]


def setup(app: Sphinx) -> ExtensionMetadata:
    """Register the "example" and "output" directives with Sphinx."""
    app.add_directive("example", ExampleDirective)
    app.add_directive("output", OutputDirective)
    return {"parallel_read_safe": True, "parallel_write_safe": True}
