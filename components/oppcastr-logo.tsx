import Image from "next/image"

interface OppcastrLogoProps {
    className?: string
    size?: number
    variant?: "icon" | "horizontal"
}

export function OppcastrLogo({ className = "", size = 32, variant = "icon" }: OppcastrLogoProps) {
    if (variant === "horizontal") {
        // Horizontal logo aspect ratio (563x127)
        const height = size
        const width = Math.round(height * (563 / 127))
        return (
            <Image
                src="/oppcastr-logo-horizontal.png"
                alt="Oppcastr"
                width={width}
                height={height}
                className={className}
                style={{ width: "100%", maxWidth: width, height: "auto", objectFit: "contain" }}
            />
        )
    }
    return (
        <Image
            src="/oppcastr-icon.png"
            alt="Oppcastr"
            width={size}
            height={size}
            className={className}
            style={{ width: "100%", maxWidth: size, height: "auto", objectFit: "contain" }}
        />
    )
}
